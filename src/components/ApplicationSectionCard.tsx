import React, { useEffect, useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import { getFieldIcon, getSectionIcon } from './DataDisplayComponents';
import ApplicationFieldRow from './ApplicationFieldRow';
import { getSectionCountLabel } from './applicationDiffCopy';
import {
  countVisibleFieldsInSection,
  fieldMatchesCurrentDiff,
  fieldMatchesHistoryDiff,
  hasVisibleFieldsInSection,
  matchesDiffFilter,
  type ApplicationDiffFilterMode,
} from './applicationDiffUtils';

interface SectionMetadata {
  profile_version?: number;
  profile_updated_at?: string;
}

interface FieldSourceInfo {
  label: string;
  detail: string;
}

interface ApplicationSectionCardProps {
  title: string;
  sectionPath: string;
  data: Record<string, unknown>;
  editMode: boolean;
  diffFilter: ApplicationDiffFilterMode;
  historyDiffBulkAction?: { mode: 'expand' | 'collapse'; token: number };
  sectionBulkAction?: { mode: 'expand' | 'collapse'; token: number };
  activeDiffRowKey?: string | null;
  onFieldChange: (sectionTitle: string, fieldName: string, value: string) => void;
  metadata?: SectionMetadata;
  currentSavedData?: Record<string, unknown>;
  previousSavedData?: Record<string, unknown> | null;
  historyDiffStorageKeyBase?: string;
  buildFieldSourceInfo: (
    fieldName: string,
    value: unknown,
    metadata?: SectionMetadata,
  ) => FieldSourceInfo;
}

const ApplicationSectionCard: React.FC<ApplicationSectionCardProps> = ({
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
  buildFieldSourceInfo,
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
  }, [currentSavedData, editMode, entries, historyDiffBulkAction, previousSavedData, sectionPath]);

  useEffect(() => {
    if (!sectionBulkAction) {
      return;
    }
    setIsExpanded(sectionBulkAction.mode === 'expand');
  }, [sectionBulkAction]);

  if (entries.length === 0 && nestedEntries.length === 0) {
    return null;
  }

  return (
    <div className="overflow-hidden rounded-lg border border-gray-200 bg-white">
      <div
        className="cursor-pointer border-b border-gray-100 bg-gradient-to-r from-slate-50 to-gray-50 px-3 py-2"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-md bg-blue-100 text-blue-600">
              {getSectionIcon(title)}
            </div>
            <span className="text-sm font-medium text-gray-700">{title}</span>
            <span className="text-xs text-gray-400">
              {getSectionCountLabel(sectionStats.visible, sectionStats.total, editMode && diffFilter !== 'all')}
            </span>
          </div>
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-gray-400" />
          ) : (
            <ChevronRight className="h-4 w-4 text-gray-400" />
          )}
        </div>
      </div>

      {isExpanded ? (
        <div className="space-y-3 p-3">
          {entries.length > 0 ? (
            <div className="overflow-hidden rounded-lg border border-gray-200">
              <table className="w-full text-sm">
                <tbody>
                  {entries.map(([key, value], idx) => {
                    const sourceInfo = buildFieldSourceInfo(key, value, metadata);
                    const rowKey = `${sectionPath}::${key}`;
                    const showSourceDetail = expandedSourceKey === rowKey;
                    const showHistoryDiff = expandedHistoryDiffKeys.includes(rowKey);
                    const previousSavedValue = String(previousSavedData?.[key] ?? '');
                    const currentSavedValue = String(currentSavedData?.[key] ?? value ?? '');
                    const currentEditingValue = String(value ?? '');
                    const hasPreviousSavedDiff = fieldMatchesHistoryDiff(previousSavedValue, currentSavedValue);
                    const modified = fieldMatchesCurrentDiff(currentSavedValue, currentEditingValue);
                    const isDiffTarget =
                      diffFilter === 'all'
                        ? hasPreviousSavedDiff || modified
                        : diffFilter === 'history'
                          ? hasPreviousSavedDiff
                          : modified;

                    return (
                      <ApplicationFieldRow
                        key={key}
                        title={title}
                        fieldName={key}
                        rowKey={rowKey}
                        rowIndex={idx}
                        value={value}
                        editMode={editMode}
                        currentEditingValue={currentEditingValue}
                        currentSavedValue={currentSavedValue}
                        previousSavedValue={previousSavedValue}
                        modified={modified}
                        hasPreviousSavedDiff={hasPreviousSavedDiff}
                        isDiffTarget={isDiffTarget}
                        isActiveDiffTarget={activeDiffRowKey === rowKey}
                        showSourceDetail={showSourceDetail}
                        showHistoryDiff={showHistoryDiff}
                        sourceInfo={sourceInfo}
                        fieldIcon={getFieldIcon(key)}
                        onFieldChange={(nextValue) => onFieldChange(title, key, nextValue)}
                        onToggleSourceDetail={() =>
                          setExpandedSourceKey((prev) => (prev === rowKey ? null : rowKey))
                        }
                        onToggleHistoryDiff={() =>
                          setExpandedHistoryDiffKeys((prev) =>
                            prev.includes(rowKey)
                              ? prev.filter((item) => item !== rowKey)
                              : [...prev, rowKey],
                          )
                        }
                      />
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}

          {nestedEntries.map(([key, value]) => (
            <ApplicationSectionCard
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
              buildFieldSourceInfo={buildFieldSourceInfo}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
};

export default ApplicationSectionCard;
