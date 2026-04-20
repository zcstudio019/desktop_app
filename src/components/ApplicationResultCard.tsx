import React, { useCallback, useEffect, useState } from 'react';
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
import { getFieldIcon, getSectionIcon, formatTableValue } from './DataDisplayComponents';
import FieldDiffPreview from './FieldDiffPreview';
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

type ApplicationDiffFilterMode = 'all' | 'current' | 'history';
type ApplicationDiffCatalogFilterMode = 'all' | 'current' | 'history' | 'both';
const APPLICATION_DIFF_FILTER_STORAGE_PREFIX = 'loan-assistant:application-diff-filter:';
const APPLICATION_HISTORY_DIFF_STORAGE_PREFIX = 'loan-assistant:application-history-diff:';

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
        --line: #e2e8f0;
        --text: #0f172a;
        --muted: #64748b;
        --accent: #f59e0b;
        --accent-soft: #fff7ed;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        padding: 32px;
        background: var(--bg);
        color: var(--text);
        font-family: "PingFang SC", "Microsoft YaHei", sans-serif;
      }
      .page {
        max-width: 960px;
        margin: 0 auto;
      }
      .hero {
        background: linear-gradient(135deg, #fff7ed 0%, #fffbeb 100%);
        border: 1px solid #fed7aa;
        border-radius: 24px;
        padding: 28px;
        margin-bottom: 24px;
      }
      .hero h1 {
        margin: 0;
        font-size: 28px;
        line-height: 1.2;
      }
      .hero p {
        margin: 12px 0 0;
        color: var(--muted);
        font-size: 15px;
      }
      .hero-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 18px;
      }
      .chip {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255,255,255,0.92);
        border: 1px solid #fde68a;
        color: #92400e;
        font-size: 13px;
      }
      .section-stack {
        display: grid;
        gap: 16px;
      }
      .section-card {
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: 20px;
        overflow: hidden;
        box-shadow: 0 12px 32px rgba(15, 23, 42, 0.05);
      }
      .section-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 18px 20px;
        background: linear-gradient(135deg, #f8fafc 0%, #ffffff 100%);
        border-bottom: 1px solid var(--line);
      }
      .section-title {
        font-size: 18px;
        font-weight: 700;
      }
      .section-count {
        color: var(--muted);
        font-size: 13px;
      }
      .table-shell {
        padding: 0 20px 20px;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      th, td {
        border-bottom: 1px solid #edf2f7;
        padding: 14px 12px;
        vertical-align: top;
        text-align: left;
      }
      th {
        width: 32%;
        color: var(--muted);
        font-weight: 600;
        background: #fcfdff;
      }
      tr:last-child th, tr:last-child td {
        border-bottom: none;
      }
      .footer {
        margin-top: 24px;
        color: var(--muted);
        font-size: 12px;
        text-align: center;
      }
      @media (max-width: 720px) {
        body { padding: 18px; }
        .hero { padding: 20px; }
        .section-header {
          flex-direction: column;
          align-items: flex-start;
          gap: 6px;
        }
        table, tbody, tr, th, td { display: block; width: 100%; }
        th {
          border-bottom: none;
          padding-bottom: 6px;
          background: transparent;
        }
        td {
          padding-top: 0;
        }
      }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <h1>${escapeHtml(safeCustomerName)} · ${escapeHtml(loanTypeLabel)} 申请表</h1>
        <p>导出时间：${escapeHtml(exportedAt)}。当前文件为结构化申请表导出版本，便于补充、核对与归档。</p>
        <div class="hero-meta">
          <span class="chip">客户：${escapeHtml(safeCustomerName)}</span>
          <span class="chip">融资类型：${escapeHtml(loanTypeLabel)}</span>
          <span class="chip">导出方式：网页表单</span>
        </div>
      </section>
      <section class="section-stack">
        ${sectionsHtml}
      </section>
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

function matchesDiffFilter(
  diffFilter: ApplicationDiffFilterMode,
  previousSavedValue: string,
  currentSavedValue: string,
  currentEditingValue: string,
): boolean {
  const hasPreviousSavedDiff = previousSavedValue !== '' && previousSavedValue !== currentSavedValue;
  const modified = currentSavedValue !== currentEditingValue;

  if (diffFilter === 'current') return modified;
  if (diffFilter === 'history') return hasPreviousSavedDiff;
  return true;
}

function hasVisibleFieldsInSection(
  sectionData: Record<string, unknown>,
  currentSavedData: Record<string, unknown>,
  previousSavedData: Record<string, unknown> | null,
  diffFilter: ApplicationDiffFilterMode,
): boolean {
  if (diffFilter === 'all') {
    return true;
  }

  return Object.entries(sectionData).some(([key, value]) => {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      return hasVisibleFieldsInSection(
        value as Record<string, unknown>,
        (currentSavedData?.[key] as Record<string, unknown>) || {},
        (previousSavedData?.[key] as Record<string, unknown>) || null,
        diffFilter,
      );
    }

    return matchesDiffFilter(
      diffFilter,
      String(previousSavedData?.[key] ?? ''),
      String(currentSavedData?.[key] ?? value ?? ''),
      String(value ?? ''),
    );
  });
}

function countDiffStats(
  applicationData: Record<string, Record<string, unknown>>,
  currentSavedData: Record<string, Record<string, unknown>>,
  previousSavedData: Record<string, Record<string, unknown>> | null,
): { total: number; current: number; history: number } {
  let total = 0;
  let current = 0;
  let history = 0;

  const walk = (
    sectionData: Record<string, unknown>,
    currentSection: Record<string, unknown>,
    previousSection: Record<string, unknown> | null,
  ) => {
    Object.entries(sectionData).forEach(([key, value]) => {
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        walk(
          value as Record<string, unknown>,
          (currentSection?.[key] as Record<string, unknown>) || {},
          (previousSection?.[key] as Record<string, unknown>) || null,
        );
        return;
      }

      total += 1;
      const previousSavedValue = String(previousSection?.[key] ?? '');
      const currentSavedValue = String(currentSection?.[key] ?? value ?? '');
      const currentEditingValue = String(value ?? '');
      if (currentSavedValue !== currentEditingValue) current += 1;
      if (previousSavedValue !== '' && previousSavedValue !== currentSavedValue) history += 1;
    });
  };

  Object.entries(applicationData).forEach(([sectionName, sectionData]) => {
    walk(
      sectionData,
      (currentSavedData?.[sectionName] as Record<string, unknown>) || {},
      (previousSavedData?.[sectionName] as Record<string, unknown>) || null,
    );
  });

  return { total, current, history };
}

function countVisibleFieldsInSection(
  sectionData: Record<string, unknown>,
  currentSavedData: Record<string, unknown>,
  previousSavedData: Record<string, unknown> | null,
  diffFilter: ApplicationDiffFilterMode,
): { visible: number; total: number } {
  let visible = 0;
  let total = 0;

  Object.entries(sectionData).forEach(([key, value]) => {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const nested = countVisibleFieldsInSection(
        value as Record<string, unknown>,
        (currentSavedData?.[key] as Record<string, unknown>) || {},
        (previousSavedData?.[key] as Record<string, unknown>) || null,
        diffFilter,
      );
      visible += nested.visible;
      total += nested.total;
      return;
    }

    total += 1;
    if (
      matchesDiffFilter(
        diffFilter,
        String(previousSavedData?.[key] ?? ''),
        String(currentSavedData?.[key] ?? value ?? ''),
        String(value ?? ''),
      )
    ) {
      visible += 1;
    }
  });

  return { visible, total };
}

function fieldMatchesCurrentDiff(
  currentSavedValue: string,
  currentEditingValue: string,
): boolean {
  return currentSavedValue !== currentEditingValue;
}

function fieldMatchesHistoryDiff(
  previousSavedValue: string,
  currentSavedValue: string,
): boolean {
  return previousSavedValue !== '' && previousSavedValue !== currentSavedValue;
}

function collectDiffTargets(
  sectionPath: string,
  sectionData: Record<string, unknown>,
  currentSavedData: Record<string, unknown>,
  previousSavedData: Record<string, unknown> | null,
  diffFilter: ApplicationDiffFilterMode,
): Array<{
  rowKey: string;
  groupKey: string;
  label: string;
  shortLabel: string;
  kind: 'current' | 'history' | 'both';
  tooltip: string;
}> {
  const targets: Array<{
    rowKey: string;
    groupKey: string;
    label: string;
    shortLabel: string;
    kind: 'current' | 'history' | 'both';
    tooltip: string;
  }> = [];

  Object.entries(sectionData).forEach(([key, value]) => {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      targets.push(
        ...collectDiffTargets(
          `${sectionPath}.${key}`,
          value as Record<string, unknown>,
          (currentSavedData?.[key] as Record<string, unknown>) || {},
          (previousSavedData?.[key] as Record<string, unknown>) || null,
          diffFilter,
        ),
      );
      return;
    }

    const previousSavedValue = String(previousSavedData?.[key] ?? '');
    const currentSavedValue = String(currentSavedData?.[key] ?? value ?? '');
    const currentEditingValue = String(value ?? '');
    const hasHistoryDiff = fieldMatchesHistoryDiff(previousSavedValue, currentSavedValue);
    const hasCurrentDiff = fieldMatchesCurrentDiff(currentSavedValue, currentEditingValue);
    const matches =
      diffFilter === 'all'
        ? hasHistoryDiff || hasCurrentDiff
        : diffFilter === 'history'
          ? hasHistoryDiff
          : hasCurrentDiff;

    if (!matches) {
      return;
    }

    const tooltipParts: string[] = [`字段：${sectionPath.replace(/\./g, ' / ')} / ${key}`];
    if (hasHistoryDiff) {
      tooltipParts.push(`上一版本：${previousSavedValue || '（空）'}`);
      tooltipParts.push(`当前保存：${currentSavedValue || '（空）'}`);
    }
    if (hasCurrentDiff) {
      tooltipParts.push(`当前保存：${currentSavedValue || '（空）'}`);
      tooltipParts.push(`当前编辑：${currentEditingValue || '（空）'}`);
    }

    targets.push({
      rowKey: `${sectionPath}::${key}`,
      groupKey: sectionPath.split('.')[0] || sectionPath,
      label: `${sectionPath.replace(/\./g, ' / ')} / ${key}`,
      shortLabel: key,
      kind: hasHistoryDiff && hasCurrentDiff ? 'both' : hasHistoryDiff ? 'history' : 'current',
      tooltip: tooltipParts.join('\n'),
    });
  });

  return targets;
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

interface EditableDataSectionCardChatProps {
  title: string;
  sectionPath: string;
  data: Record<string, unknown>;
  editMode: boolean;
  diffFilter: ApplicationDiffFilterMode;
  historyDiffBulkAction?: { mode: 'expand' | 'collapse'; token: number };
  sectionBulkAction?: { mode: 'expand' | 'collapse'; token: number };
  activeDiffRowKey?: string | null;
  onFieldChange: (sectionTitle: string, fieldName: string, value: string) => void;
  metadata?: ApplicationResultCardData['metadata'];
  currentSavedData?: Record<string, unknown>;
  previousSavedData?: Record<string, unknown> | null;
  historyDiffStorageKeyBase?: string;
}

const EditableDataSectionCardChat: React.FC<EditableDataSectionCardChatProps> = ({
  title,
  sectionPath,
  data,
  editMode,
  diffFilter,
  historyDiffBulkAction,
  sectionBulkAction,
  activeDiffRowKey,
  onFieldChange,
  metadata,
  currentSavedData = {},
  previousSavedData = null,
  historyDiffStorageKeyBase,
}) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [expandedSourceKey, setExpandedSourceKey] = useState<string | null>(null);
  const [expandedHistoryDiffKeys, setExpandedHistoryDiffKeys] = useState<string[]>([]);
  const expandedHistoryStorageKey = historyDiffStorageKeyBase
    ? `${historyDiffStorageKeyBase}:${sectionPath}`
    : '';
  const allEntries = Object.entries(data).filter(([, value]) => typeof value !== 'object' || value === null);
  const allNestedEntries = Object.entries(data).filter(
    ([, value]) => typeof value === 'object' && value !== null && !Array.isArray(value),
  );
  const entries =
    editMode && diffFilter !== 'all'
      ? allEntries.filter(([key, value]) =>
          matchesDiffFilter(
            diffFilter,
            String(previousSavedData?.[key] ?? ''),
            String(currentSavedData?.[key] ?? value ?? ''),
            String(value ?? ''),
          ),
        )
      : allEntries;
  const nestedEntries =
    editMode && diffFilter !== 'all'
      ? allNestedEntries.filter(([key, value]) =>
          hasVisibleFieldsInSection(
            value as Record<string, unknown>,
            (currentSavedData?.[key] as Record<string, unknown>) || {},
            (previousSavedData?.[key] as Record<string, unknown>) || null,
            diffFilter,
          ),
        )
      : allNestedEntries;
  const sectionStats = countVisibleFieldsInSection(data, currentSavedData, previousSavedData, diffFilter);

  useEffect(() => {
    if (!expandedHistoryStorageKey || typeof window === 'undefined') {
      return;
    }
    const storedValue = window.localStorage.getItem(expandedHistoryStorageKey);
    if (!storedValue) {
      setExpandedHistoryDiffKeys([]);
      return;
    }
    try {
      const parsed = JSON.parse(storedValue);
      if (Array.isArray(parsed)) {
        setExpandedHistoryDiffKeys(parsed.filter((item): item is string => typeof item === 'string'));
        return;
      }
    } catch {}
    setExpandedHistoryDiffKeys([storedValue]);
  }, [expandedHistoryStorageKey]);

  useEffect(() => {
    if (!expandedHistoryStorageKey || typeof window === 'undefined') {
      return;
    }
    if (expandedHistoryDiffKeys.length > 0) {
      window.localStorage.setItem(expandedHistoryStorageKey, JSON.stringify(expandedHistoryDiffKeys));
    } else {
      window.localStorage.removeItem(expandedHistoryStorageKey);
    }
  }, [expandedHistoryDiffKeys, expandedHistoryStorageKey]);

  useEffect(() => {
    if (!historyDiffBulkAction || !editMode) {
      return;
    }
    if (historyDiffBulkAction.mode === 'collapse') {
      setExpandedHistoryDiffKeys([]);
      return;
    }
        const nextExpandedKeys = entries
      .filter(([key, value]) => {
        const previousSavedValue = String(previousSavedData?.[key] ?? '');
        const currentSavedValue = String(currentSavedData?.[key] ?? value ?? '');
        return previousSavedValue !== '' && previousSavedValue !== currentSavedValue;
      })
      .map(([key]) => `${sectionPath}::${key}`);
    setExpandedHistoryDiffKeys(nextExpandedKeys);
  }, [
    currentSavedData,
    editMode,
    entries,
    historyDiffBulkAction,
    previousSavedData,
    title,
  ]);

  useEffect(() => {
    if (!sectionBulkAction) {
      return;
    }
    setIsExpanded(sectionBulkAction.mode === 'expand');
  }, [sectionBulkAction]);

  if (entries.length === 0 && nestedEntries.length === 0) return null;

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
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
            <span className="text-xs text-gray-400">
              {editMode && diffFilter !== 'all'
                ? `(${sectionStats.visible}/${sectionStats.total} 项)`
                : `(${sectionStats.total} 项)`}
            </span>
          </div>
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-400" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          )}
        </div>
      </div>

      {isExpanded && (
        <div className="p-3 space-y-3">
          {entries.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-gray-200">
              <table className="w-full text-sm">
                <tbody>
                  {entries.map(([key, value], idx) => {
                    const sourceInfo = buildChatApplicationFieldSource(key, value, metadata);
                    const rowKey = `${sectionPath}::${key}`;
                    const showSourceDetail = expandedSourceKey === rowKey;
                    const showHistoryDiff = expandedHistoryDiffKeys.includes(rowKey);
                    const previousSavedValue = String(previousSavedData?.[key] ?? '');
                    const currentSavedValue = String(currentSavedData?.[key] ?? value ?? '');
                    const currentEditingValue = String(value ?? '');
                    const hasPreviousSavedDiff = fieldMatchesHistoryDiff(previousSavedValue, currentSavedValue);
                    const modified = fieldMatchesCurrentDiff(currentSavedValue, currentEditingValue);
                    const isDiffTarget = diffFilter === 'all'
                      ? hasPreviousSavedDiff || modified
                      : diffFilter === 'history'
                        ? hasPreviousSavedDiff
                        : modified;
                    const isActiveDiffTarget = activeDiffRowKey === rowKey;

                    return (
                      <tr
                        key={key}
                        data-diff-row={isDiffTarget ? 'true' : 'false'}
                        data-diff-row-key={rowKey}
                        data-current-diff={modified ? 'true' : 'false'}
                        data-history-diff={hasPreviousSavedDiff ? 'true' : 'false'}
                        className={`${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'} ${
                          isActiveDiffTarget ? 'ring-2 ring-blue-200 ring-inset bg-blue-50/40' : ''
                        }`}
                      >
                        <td className="px-3 py-2 text-gray-500 font-medium w-1/3 border-r border-gray-100">
                          <div className="flex items-center gap-2">
                            <span className="text-gray-400">{getFieldIcon(key)}</span>
                            <span className="truncate">{key}</span>
                          </div>
                        </td>
                        <td className="px-3 py-2 text-gray-800">
                          {editMode ? (
                            <div className="space-y-2">
                              <input
                                type="text"
                                value={currentEditingValue}
                                onChange={(e) => onFieldChange(title, key, e.target.value)}
                                className={`w-full rounded border px-2 py-1 text-sm focus:outline-none focus:ring-2 ${
                                  modified
                                    ? 'border-amber-300 bg-amber-50/50 focus:ring-amber-100'
                                    : 'border-blue-300 focus:ring-blue-200'
                                }`}
                                data-testid={`edit-field-${title}-${key}`}
                              />
                              {hasPreviousSavedDiff ? (
                                <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-2.5 py-2">
                                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                                    <div className="flex min-w-0 flex-wrap items-center gap-2">
                                      <span className="inline-flex h-2 w-2 rounded-full bg-amber-400" aria-hidden="true" />
                                      <span className="text-[11px] font-semibold tracking-wide text-slate-700">上一版变更记录</span>
                                      <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-[11px] font-medium text-amber-700">
                                        检测到历史差异
                                      </span>
                                    </div>
                                    <button
                                      type="button"
                                      onClick={() =>
                                        setExpandedHistoryDiffKeys((prev) =>
                                          prev.includes(rowKey)
                                            ? prev.filter((item) => item !== rowKey)
                                            : [...prev, rowKey],
                                        )
                                      }
                                      className="inline-flex w-fit items-center rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                                    >
                                      {showHistoryDiff ? '收起上一版差异' : '查看上一版差异'}
                                    </button>
                                  </div>
                                  {showHistoryDiff ? (
                                    <div className="mt-2">
                                      <FieldDiffPreview originalValue={previousSavedValue} currentValue={currentSavedValue} />
                                    </div>
                                  ) : (
                                    <div className="mt-2 rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-500">
                                      已检测到上一保存版本差异，点击“查看上一版差异”可展开详情。
                                    </div>
                                  )}
                                </div>
                              ) : null}
                              <div
                                className={`rounded-lg border border-dashed px-2.5 py-2 ${
                                  modified
                                    ? 'border-amber-300 bg-amber-50/70'
                                    : 'border-slate-300 bg-slate-50'
                                }`}
                              >
                                <div className="mb-2 flex min-w-0 flex-wrap items-center gap-2">
                                  <span
                                    className={`inline-flex h-2 w-2 rounded-full ${modified ? 'bg-emerald-500' : 'bg-slate-300'}`}
                                    aria-hidden="true"
                                  />
                                  <span className={`text-[11px] font-semibold tracking-wide ${modified ? 'text-amber-800' : 'text-slate-700'}`}>
                                    本次编辑差异
                                  </span>
                                  <span
                                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${
                                      modified ? 'bg-emerald-100 text-emerald-700' : 'bg-white text-slate-500'
                                    }`}
                                  >
                                    {modified ? '已检测到本次修改' : '编辑中'}
                                  </span>
                                </div>
                                {modified ? (
                                  <div className="mt-2">
                                    <FieldDiffPreview originalValue={currentSavedValue} currentValue={currentEditingValue} />
                                  </div>
                                ) : (
                                  <div className="mt-2 rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-500">
                                    本次编辑暂无差异。
                                  </div>
                                )}
                              </div>
                            </div>
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

          {nestedEntries.map(([key, value]) => (
            <EditableDataSectionCardChat
              key={key}
              title={key}
              sectionPath={`${sectionPath}.${key}`}
              data={value as Record<string, unknown>}
              editMode={editMode}
              diffFilter={diffFilter}
              historyDiffBulkAction={historyDiffBulkAction}
              sectionBulkAction={sectionBulkAction}
              activeDiffRowKey={activeDiffRowKey}
              onFieldChange={onFieldChange}
              metadata={metadata}
              currentSavedData={(currentSavedData?.[key] as Record<string, unknown>) || {}}
              previousSavedData={(previousSavedData?.[key] as Record<string, unknown>) || null}
              historyDiffStorageKeyBase={historyDiffStorageKeyBase}
            />
          ))}
        </div>
      )}
    </div>
  );
};

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

  return (
    <div className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm" data-testid="application-guide-card">
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

export const ApplicationResultCard: React.FC<ApplicationResultCardProps> = ({
  data,
  relatedJobId,
  onNavigate,
  onPersistMessageData,
  previousApplicationCache,
  onCachePreviousApplication,
}) => {
  const { state, setApplicationResult, updateChatMessagesByJob } = useApp();
  const [isExpanded, setIsExpanded] = useState(true);
  const [editMode, setEditMode] = useState(false);
  const [editedData, setEditedData] = useState<Record<string, Record<string, unknown>>>({});
  const [savedData, setSavedData] = useState<Record<string, Record<string, unknown>> | null>(null);
  const [previousSavedData, setPreviousSavedData] = useState<Record<string, Record<string, unknown>> | null>(null);
  const [savedApplicationId, setSavedApplicationId] = useState('');
  const [savedPreviousApplicationId, setSavedPreviousApplicationId] = useState('');
  const [savedVersionGroupId, setSavedVersionGroupId] = useState('');
  const [savingEdit, setSavingEdit] = useState(false);
  const [saveEditError, setSaveEditError] = useState<string | null>(null);
  const [diffFilter, setDiffFilter] = useState<ApplicationDiffFilterMode>('all');
  const [historyDiffBulkAction, setHistoryDiffBulkAction] = useState<{
    mode: 'expand' | 'collapse';
    token: number;
  }>({ mode: 'collapse', token: 0 });
  const [sectionBulkAction, setSectionBulkAction] = useState<{
    mode: 'expand' | 'collapse';
    token: number;
  }>({ mode: 'expand', token: 0 });
  const [activeDiffRowKey, setActiveDiffRowKey] = useState<string | null>(null);
  const [pendingScrollRowKey, setPendingScrollRowKey] = useState<string | null>(null);
  const [diffCatalogFilter, setDiffCatalogFilter] = useState<ApplicationDiffCatalogFilterMode>('all');

  const handleFieldChange = useCallback((sectionTitle: string, fieldName: string, value: string) => {
    setEditedData((prev) => ({
      ...prev,
      [sectionTitle]: {
        ...(prev[sectionTitle] || {}),
        [fieldName]: value,
      },
    }));
  }, []);

  if (data.needsInput) {
    return <ApplicationGuideCard data={data} onNavigate={onNavigate} />;
  }

  if (!data.applicationData && !data.applicationContent) {
    return <ApplicationGuideCard data={data} onNavigate={onNavigate} />;
  }

  const loanTypeLabel = data.loanType === 'personal' ? '个人贷款' : '企业贷款';
  const hasStructuredData = Boolean(data.applicationData && Object.keys(data.applicationData).length > 0);
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
  const stableCustomerId = data.metadata?.customer_id || state.extraction.currentCustomerId || null;
  const diffFilterStorageKey = [
    APPLICATION_DIFF_FILTER_STORAGE_PREFIX,
    data.metadata?.saved_application_version_group_id || '',
    data.metadata?.saved_application_id || '',
    stableCustomerId || '',
    data.customerName || '',
  ]
    .filter(Boolean)
    .join('');
  const historyDiffStorageKeyBase = [
    APPLICATION_HISTORY_DIFF_STORAGE_PREFIX,
    data.metadata?.saved_application_version_group_id || '',
    data.metadata?.saved_application_id || '',
    stableCustomerId || '',
    data.customerName || '',
  ]
    .filter(Boolean)
    .join('');

  const currentSavedData = savedData || data.applicationData || {};
  const displayData = editMode && Object.keys(editedData).length > 0 ? editedData : currentSavedData;
  const diffStats = countDiffStats(displayData, currentSavedData, previousSavedData);
  const hasVisibleFieldsForCurrentFilter =
    diffFilter === 'all'
      ? true
      : Object.entries(displayData).some(([sectionName, sectionData]) =>
          hasVisibleFieldsInSection(
            sectionData,
            (currentSavedData?.[sectionName] as Record<string, unknown>) || {},
            (previousSavedData?.[sectionName] as Record<string, unknown>) || null,
            diffFilter,
          ),
        );
  const diffTargets = Object.entries(displayData).flatMap(([sectionName, sectionData]) =>
    collectDiffTargets(
      sectionName,
      sectionData,
      (currentSavedData?.[sectionName] as Record<string, unknown>) || {},
      (previousSavedData?.[sectionName] as Record<string, unknown>) || null,
      diffFilter,
    ),
  );
  const groupedDiffTargets = diffTargets.reduce<
    Array<{
      groupKey: string;
      count: number;
      currentCount: number;
      historyCount: number;
      bothCount: number;
      items: typeof diffTargets;
    }>
  >((groups, target) => {
    const existingGroup = groups.find((group) => group.groupKey === target.groupKey);
    if (existingGroup) {
      existingGroup.items.push(target);
      existingGroup.count += 1;
      if (target.kind === 'both') {
        existingGroup.bothCount += 1;
      } else if (target.kind === 'history') {
        existingGroup.historyCount += 1;
      } else {
        existingGroup.currentCount += 1;
      }
      return groups;
    }

    groups.push({
      groupKey: target.groupKey,
      count: 1,
      currentCount: target.kind === 'current' ? 1 : 0,
      historyCount: target.kind === 'history' ? 1 : 0,
      bothCount: target.kind === 'both' ? 1 : 0,
      items: [target],
    });
    return groups;
  }, []);
  const filteredGroupedDiffTargets = groupedDiffTargets
    .map((group) => ({
      ...group,
      items:
        diffCatalogFilter === 'all'
          ? group.items
          : group.items.filter((item) => item.kind === diffCatalogFilter),
    }))
    .filter((group) => group.items.length > 0);

  const navigateDiffField = useCallback(
    (direction: 'next' | 'prev') => {
      if (typeof document === 'undefined') {
        return;
      }
      const card = document.querySelector('[data-testid="application-result-card"]');
      if (!card) {
        return;
      }
      const diffRows = Array.from(
        card.querySelectorAll<HTMLTableRowElement>('tr[data-diff-row="true"]'),
      );
      if (diffRows.length === 0) {
        setActiveDiffRowKey(null);
        return;
      }
      const currentIndex = activeDiffRowKey
        ? diffRows.findIndex((row) => row.dataset.diffRowKey === activeDiffRowKey)
        : -1;
      const nextIndex =
        direction === 'next'
          ? (currentIndex + 1 + diffRows.length) % diffRows.length
          : (currentIndex - 1 + diffRows.length) % diffRows.length;
      const nextRow = diffRows[nextIndex];
      const nextKey = nextRow.dataset.diffRowKey || null;
      setActiveDiffRowKey(nextKey);
      nextRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
    },
    [activeDiffRowKey],
  );

  useEffect(() => {
    setActiveDiffRowKey(null);
  }, [diffFilter, editMode, data.metadata?.saved_application_id]);

  useEffect(() => {
    setDiffCatalogFilter('all');
  }, [diffFilter, data.metadata?.saved_application_id]);

  useEffect(() => {
    if (!pendingScrollRowKey || typeof document === 'undefined') {
      return;
    }
    const card = document.querySelector('[data-testid="application-result-card"]');
    if (!card) {
      return;
    }
    const targetRow = card.querySelector<HTMLTableRowElement>(
      `tr[data-diff-row-key="${pendingScrollRowKey}"]`,
    );
    if (!targetRow) {
      return;
    }
    setActiveDiffRowKey(pendingScrollRowKey);
    targetRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
    setPendingScrollRowKey(null);
  }, [pendingScrollRowKey, sectionBulkAction.token, historyDiffBulkAction.token]);

  useEffect(() => {
    setSavedApplicationId(data.metadata?.saved_application_id || '');
    setSavedPreviousApplicationId(data.metadata?.previous_application_id || '');
    setSavedVersionGroupId(data.metadata?.saved_application_version_group_id || '');
  }, [
    data.metadata?.previous_application_id,
    data.metadata?.saved_application_id,
    data.metadata?.saved_application_version_group_id,
  ]);

  useEffect(() => {
    if (!diffFilterStorageKey || typeof window === 'undefined') {
      return;
    }
    const storedValue = window.localStorage.getItem(diffFilterStorageKey);
    if (storedValue === 'all' || storedValue === 'current' || storedValue === 'history') {
      setDiffFilter(storedValue);
    }
  }, [diffFilterStorageKey]);

  useEffect(() => {
    if (!diffFilterStorageKey || typeof window === 'undefined') {
      return;
    }
    window.localStorage.setItem(diffFilterStorageKey, diffFilter);
  }, [diffFilter, diffFilterStorageKey]);

  useEffect(() => {
    if (!savedPreviousApplicationId) {
      return;
    }

    const cachedPreviousApplication = previousApplicationCache?.[savedPreviousApplicationId];
    if (cachedPreviousApplication) {
      setPreviousSavedData(cloneChatApplicationData(cachedPreviousApplication));
      return;
    }

    const controller = new AbortController();
    void (async () => {
      try {
        const previousApplication = await getApplication(savedPreviousApplicationId, controller.signal);
        const previousApplicationData = cloneChatApplicationData(
          previousApplication.applicationData as Record<string, Record<string, unknown>>,
        );
        setPreviousSavedData(previousApplicationData);
        onCachePreviousApplication?.(savedPreviousApplicationId, previousApplicationData);
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') {
          return;
        }
        console.warn('Failed to load previous application version for chat card', error);
      }
    })();

    return () => controller.abort();
  }, [onCachePreviousApplication, previousApplicationCache, savedPreviousApplicationId]);

  const toggleEditMode = () => {
    if (!editMode) {
      const baseData = cloneChatApplicationData(currentSavedData);
      if (Object.keys(baseData).length > 0) {
        setEditedData(baseData);
      }
    }
    setEditMode(!editMode);
  };

  const saveEditedData = async () => {
    if (Object.keys(editedData).length === 0) {
      setEditMode(false);
      return;
    }

    setSavingEdit(true);
    setSaveEditError(null);
    try {
      const currentSnapshot = cloneChatApplicationData(currentSavedData);
      const nextSavedSnapshot = cloneChatApplicationData(editedData);
      const safeCustomerName = (data.customerName || state.extraction.currentCustomer || '').trim() || '未命名客户';
      const savedApplication = await saveApplication({
        customerName: safeCustomerName,
        customerId: stableCustomerId,
        loanType: data.loanType === 'personal' ? 'personal' : 'enterprise',
        applicationData: nextSavedSnapshot,
        baseApplicationId: savedApplicationId || undefined,
        versionGroupId: savedVersionGroupId || undefined,
      });

      if (Object.keys(currentSnapshot).length > 0) {
        setPreviousSavedData(currentSnapshot);
        if (savedApplicationId) {
          onCachePreviousApplication?.(savedApplicationId, currentSnapshot);
        }
      }
      setSavedApplicationId(savedApplication.id);
      setSavedPreviousApplicationId(savedApplication.previousApplicationId || '');
      setSavedVersionGroupId(savedApplication.versionGroupId || '');
      setSavedData(nextSavedSnapshot);
      setEditedData(nextSavedSnapshot);
      const nextMetadata: NonNullable<ApplicationResultCardData['metadata']> = {
        ...(data.metadata || {}),
        customer_id: stableCustomerId || data.metadata?.customer_id,
        saved_application_id: savedApplication.id,
        previous_application_id: savedApplication.previousApplicationId || '',
        saved_application_version_group_id: savedApplication.versionGroupId || '',
        saved_application_version_no: savedApplication.versionNo,
      };
      const nextCardData: ApplicationResultCardData = {
        ...data,
        applicationData: nextSavedSnapshot,
        metadata: nextMetadata,
      };
      setApplicationResult(
        {
          content: data.applicationContent || state.application.result?.content || '',
          customerFound: data.customerFound ?? true,
          warnings: data.warnings || [],
          applicationData: nextSavedSnapshot as Record<string, Record<string, string>>,
          metadata: nextMetadata,
        },
        data.customerName || state.application.lastCustomer || undefined,
      );
      if (relatedJobId) {
        if (onPersistMessageData) {
          onPersistMessageData(relatedJobId, nextCardData);
        } else {
          updateChatMessagesByJob(relatedJobId, {
            data: nextCardData as Record<string, unknown>,
            intent: 'application',
            messageType: 'task_result',
          });
        }
      }
      setEditMode(false);
    } catch (error) {
      setSaveEditError(error instanceof Error ? error.message : '申请表保存失败，请稍后重试。');
    } finally {
      setSavingEdit(false);
    }
  };

  const downloadJSON = () => {
    const dataToDownload = savedData || (Object.keys(editedData).length > 0 ? editedData : data.applicationData);
    if (!dataToDownload || Object.keys(dataToDownload).length === 0) return;

    const jsonContent = JSON.stringify(dataToDownload, null, 2);
    const blob = new Blob([jsonContent], { type: 'application/json;charset=utf-8' });
    createDownloadLink(blob, `贷款申请表_${data.customerName || '未命名'}_${new Date().toISOString().split('T')[0]}.json`);
  };

  void downloadJSON;

  const downloadFormHtml = () => {
    const dataToDownload = savedData || (Object.keys(editedData).length > 0 ? editedData : data.applicationData);
    if (!dataToDownload || Object.keys(dataToDownload).length === 0) return;

    const htmlContent = buildApplicationFormHtml(data.customerName || '', data.loanType || 'enterprise', dataToDownload);
    const blob = new Blob([htmlContent], { type: 'text/html;charset=utf-8' });
    createDownloadLink(blob, `贷款申请表_${data.customerName || '未命名'}_${new Date().toISOString().split('T')[0]}.html`);
  };

  return (
    <div className="mt-3 bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm" data-testid="application-result-card">
      <div className="px-4 py-3 bg-gradient-to-r from-amber-50 to-orange-50 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
              data.customerFound ? 'bg-green-100 text-green-600' : 'bg-amber-100 text-amber-600'
            }`}>
              {data.customerFound ? <CheckCircle2 className="w-5 h-5" /> : <ClipboardList className="w-5 h-5" />}
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
            {hasStructuredData &&
              (editMode ? (
                <button
                  onClick={saveEditedData}
                  disabled={savingEdit}
                  className="flex items-center gap-1 rounded-lg bg-green-500 px-2.5 py-1.5 text-xs font-medium text-white transition-colors hover:bg-green-600 disabled:cursor-not-allowed disabled:opacity-70"
                  data-testid="save-button"
                >
                  <Save className="w-3.5 h-3.5" />
                  {savingEdit ? '保存中' : '保存'}
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
              ))}
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
            <button onClick={() => setIsExpanded(!isExpanded)} className="p-1.5 hover:bg-amber-100 rounded-lg transition-colors">
              {isExpanded ? (
                <ChevronDown className="w-4 h-4 text-gray-500" />
              ) : (
                <ChevronRight className="w-4 h-4 text-gray-500" />
              )}
            </button>
          </div>
        </div>
      </div>

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

      {saveEditError ? (
        <div className="border-b border-rose-100 bg-rose-50/80 px-4 py-3 text-sm text-rose-700">{saveEditError}</div>
      ) : null}

      {editMode ? (
        <div className="border-b border-slate-100 bg-white px-4 py-3">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div className="text-sm text-slate-600">
              只看有差异字段
              <span className="ml-2 text-xs text-slate-500">
                本次修改 {diffStats.current} 项，历史差异 {diffStats.history} 项，共 {diffStats.total} 项
              </span>
            </div>
            <div className="flex flex-wrap gap-2">
              {[
                { value: 'all', label: '全部字段' },
                { value: 'current', label: `仅看本次修改${diffStats.current > 0 ? ` (${diffStats.current})` : ''}` },
                { value: 'history', label: `仅看历史差异${diffStats.history > 0 ? ` (${diffStats.history})` : ''}` },
              ].map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => setDiffFilter(option.value as ApplicationDiffFilterMode)}
                  className={`rounded-full border px-3 py-1.5 text-xs font-medium transition-colors ${
                    diffFilter === option.value
                      ? 'border-blue-500 bg-blue-50 text-blue-700'
                      : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                  }`}
                >
                  {option.label}
                </button>
              ))}
              <button
                type="button"
                onClick={() =>
                  setHistoryDiffBulkAction((prev) => ({
                    mode: 'expand',
                    token: prev.token + 1,
                  }))
                }
                disabled={diffStats.history === 0}
                className="rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-medium text-amber-700 transition-colors hover:border-amber-300 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                展开全部历史差异
              </button>
              <button
                type="button"
                onClick={() =>
                  setHistoryDiffBulkAction((prev) => ({
                    mode: 'collapse',
                    token: prev.token + 1,
                  }))
                }
                disabled={diffStats.history === 0}
                className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
              >
                收起全部历史差异
              </button>
              <button
                type="button"
                onClick={() =>
                  setSectionBulkAction((prev) => ({
                    mode: 'expand',
                    token: prev.token + 1,
                  }))
                }
                className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-800"
              >
                展开全部分组
              </button>
              <button
                type="button"
                onClick={() =>
                  setSectionBulkAction((prev) => ({
                    mode: 'collapse',
                    token: prev.token + 1,
                  }))
                }
                className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-800"
              >
                收起全部分组
              </button>
              <button
                type="button"
                onClick={() => navigateDiffField('prev')}
                disabled={
                  diffFilter === 'current'
                    ? diffStats.current === 0
                    : diffFilter === 'history'
                      ? diffStats.history === 0
                      : diffStats.current + diffStats.history === 0
                }
                className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
              >
                上一个差异字段
              </button>
              <button
                type="button"
                onClick={() => navigateDiffField('next')}
                disabled={
                  diffFilter === 'current'
                    ? diffStats.current === 0
                    : diffFilter === 'history'
                      ? diffStats.history === 0
                      : diffStats.current + diffStats.history === 0
                }
                className="rounded-full border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-medium text-blue-700 transition-colors hover:border-blue-300 hover:bg-blue-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                下一个差异字段
              </button>
            </div>
          </div>
          {groupedDiffTargets.length > 0 ? (
            <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-3">
              <div className="mb-2 flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <div className="text-xs font-semibold tracking-wide text-slate-700">差异目录</div>
                  <div className="text-[11px] text-slate-500">按分组查看差异字段，点击字段可直接定位到对应编辑项</div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {[
                    { value: 'all', label: `全部差异 (${diffTargets.length})` },
                    {
                      value: 'current',
                      label: `仅本次修改 (${diffTargets.filter((item) => item.kind === 'current').length})`,
                    },
                    {
                      value: 'history',
                      label: `仅历史差异 (${diffTargets.filter((item) => item.kind === 'history').length})`,
                    },
                    {
                      value: 'both',
                      label: `仅双重差异 (${diffTargets.filter((item) => item.kind === 'both').length})`,
                    },
                  ].map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => setDiffCatalogFilter(option.value as ApplicationDiffCatalogFilterMode)}
                      className={`rounded-full border px-3 py-1.5 text-[11px] font-medium transition-colors ${
                        diffCatalogFilter === option.value
                          ? 'border-blue-500 bg-blue-50 text-blue-700'
                          : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>
              {filteredGroupedDiffTargets.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-300 bg-white px-4 py-4 text-sm text-slate-500">
                  当前目录筛选下没有命中的差异字段，可以切换到其他目录筛选查看。
                </div>
              ) : (
                <div className="space-y-3">
                  {filteredGroupedDiffTargets.map((group) => (
                  <div key={group.groupKey} className="rounded-lg border border-slate-200 bg-white px-3 py-3">
                    <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-semibold text-slate-700">{group.groupKey}</span>
                        <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                          {group.items.length} 项
                        </span>
                      </div>
                      <div className="flex flex-wrap gap-2 text-[11px] text-slate-500">
                        {group.items.some((item) => item.kind === 'current') ? (
                          <span>本次修改 {group.items.filter((item) => item.kind === 'current').length}</span>
                        ) : null}
                        {group.items.some((item) => item.kind === 'history') ? (
                          <span>历史差异 {group.items.filter((item) => item.kind === 'history').length}</span>
                        ) : null}
                        {group.items.some((item) => item.kind === 'both') ? (
                          <span>双重差异 {group.items.filter((item) => item.kind === 'both').length}</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {group.items.map((target) => (
                        <button
                          key={target.rowKey}
                          type="button"
                          onClick={() => {
                            setSectionBulkAction((prev) => ({
                              mode: 'expand',
                              token: prev.token + 1,
                            }));
                            setPendingScrollRowKey(target.rowKey);
                          }}
                          title={target.tooltip}
                          className={`inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors ${
                            activeDiffRowKey === target.rowKey
                              ? 'border-blue-400 bg-blue-50 text-blue-700'
                              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                          }`}
                        >
                          <span
                            className={`inline-flex h-2 w-2 rounded-full ${
                              target.kind === 'both'
                                ? 'bg-violet-500'
                                : target.kind === 'history'
                                  ? 'bg-amber-400'
                                  : 'bg-emerald-500'
                            }`}
                            aria-hidden="true"
                          />
                          <span className="max-w-[14rem] truncate" title={target.label}>
                            {target.shortLabel}
                          </span>
                        </button>
                      ))}
                    </div>
                  </div>
                  ))}
                </div>
              )}
            </div>
          ) : null}
        </div>
      ) : null}

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

      {isExpanded && (
        <div className="p-4">
          {hasStructuredData ? (
            <div className="space-y-4" data-testid="application-structured-data">
              {Object.entries(displayData).map(([sectionName, sectionData]) => {
                if (typeof sectionData === 'object' && sectionData !== null && !Array.isArray(sectionData)) {
                  return (
                    <EditableDataSectionCardChat
                      key={sectionName}
                      title={sectionName}
                      sectionPath={sectionName}
                      data={sectionData as Record<string, unknown>}
                      editMode={editMode}
                      diffFilter={diffFilter}
                      historyDiffBulkAction={historyDiffBulkAction}
                      sectionBulkAction={sectionBulkAction}
                      activeDiffRowKey={activeDiffRowKey}
                      onFieldChange={handleFieldChange}
                      metadata={data.metadata}
                      currentSavedData={(savedData?.[sectionName] as Record<string, unknown>) || (data.applicationData?.[sectionName] as Record<string, unknown>) || {}}
                      previousSavedData={(previousSavedData?.[sectionName] as Record<string, unknown>) || null}
                      historyDiffStorageKeyBase={historyDiffStorageKeyBase}
                    />
                  );
                }
                return null;
              })}
              {editMode && !hasVisibleFieldsForCurrentFilter ? (
                <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
                  当前筛选下没有匹配字段，切换到“全部字段”可查看完整申请表内容。
                </div>
              ) : null}
            </div>
          ) : (
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
