import React from 'react';
import { formatTableValue } from './DataDisplayComponents';
import FieldDiffPreview from './FieldDiffPreview';
import { APPLICATION_DIFF_COPY } from './applicationDiffCopy';

interface ApplicationFieldSourceInfo {
  label: string;
  detail: string;
}

interface ApplicationFieldRowProps {
  title: string;
  fieldName: string;
  rowKey: string;
  rowIndex: number;
  value: unknown;
  editMode: boolean;
  currentEditingValue: string;
  currentSavedValue: string;
  previousSavedValue: string;
  modified: boolean;
  hasPreviousSavedDiff: boolean;
  isDiffTarget: boolean;
  isActiveDiffTarget: boolean;
  showSourceDetail: boolean;
  showHistoryDiff: boolean;
  sourceInfo: ApplicationFieldSourceInfo;
  fieldIcon: React.ReactNode;
  onFieldChange: (value: string) => void;
  onToggleSourceDetail: () => void;
  onToggleHistoryDiff: () => void;
}

const ApplicationFieldRow: React.FC<ApplicationFieldRowProps> = ({
  title,
  fieldName,
  rowKey,
  rowIndex,
  value,
  editMode,
  currentEditingValue,
  currentSavedValue,
  previousSavedValue,
  modified,
  hasPreviousSavedDiff,
  isDiffTarget,
  isActiveDiffTarget,
  showSourceDetail,
  showHistoryDiff,
  sourceInfo,
  fieldIcon,
  onFieldChange,
  onToggleSourceDetail,
  onToggleHistoryDiff,
}) => {
  return (
    <tr
      data-diff-row={isDiffTarget ? 'true' : 'false'}
      data-diff-row-key={rowKey}
      data-current-diff={modified ? 'true' : 'false'}
      data-history-diff={hasPreviousSavedDiff ? 'true' : 'false'}
      className={`${rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'} ${
        isActiveDiffTarget ? 'ring-2 ring-blue-200 ring-inset bg-blue-50/40' : ''
      }`}
    >
      <td className="w-1/3 border-r border-gray-100 px-3 py-2 font-medium text-gray-500">
        <div className="flex items-center gap-2">
          <span className="text-gray-400">{fieldIcon}</span>
          <span className="truncate">{fieldName}</span>
        </div>
      </td>
      <td className="px-3 py-2 text-gray-800">
        {editMode ? (
          <div className="space-y-2">
            <input
              type="text"
              value={currentEditingValue}
              onChange={(e) => onFieldChange(e.target.value)}
              className={`w-full rounded border px-2 py-1 text-sm focus:outline-none focus:ring-2 ${
                modified
                  ? 'border-amber-300 bg-amber-50/50 focus:ring-amber-100'
                  : 'border-blue-300 focus:ring-blue-200'
              }`}
              data-testid={`edit-field-${title}-${fieldName}`}
            />

            {hasPreviousSavedDiff ? (
              <div className="rounded-lg border border-dashed border-slate-300 bg-slate-50 px-2.5 py-2">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    <span className="inline-flex h-2 w-2 rounded-full bg-amber-400" aria-hidden="true" />
                    <span className="text-[11px] font-semibold tracking-wide text-slate-700">
                      {APPLICATION_DIFF_COPY.historySectionTitle}
                    </span>
                    <span className="inline-flex items-center rounded-full bg-amber-100 px-2 py-0.5 text-[11px] font-medium text-amber-700">
                      {APPLICATION_DIFF_COPY.historyDetectedBadge}
                    </span>
                  </div>
                  <button
                    type="button"
                    onClick={onToggleHistoryDiff}
                    className="inline-flex w-fit items-center rounded-full border border-slate-200 bg-white px-2.5 py-1 text-[11px] font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-800"
                  >
                    {showHistoryDiff
                      ? APPLICATION_DIFF_COPY.historyCollapseButton
                      : APPLICATION_DIFF_COPY.historyExpandButton}
                  </button>
                </div>
                {showHistoryDiff ? (
                  <div className="mt-2">
                    <FieldDiffPreview originalValue={previousSavedValue} currentValue={currentSavedValue} />
                  </div>
                ) : (
                  <div className="mt-2 rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-500">
                    {APPLICATION_DIFF_COPY.historyCollapsedHint}
                  </div>
                )}
              </div>
            ) : null}

            <div
              className={`rounded-lg border border-dashed px-2.5 py-2 ${
                modified ? 'border-amber-300 bg-amber-50/70' : 'border-slate-300 bg-slate-50'
              }`}
            >
              <div className="mb-2 flex min-w-0 flex-wrap items-center gap-2">
                <span
                  className={`inline-flex h-2 w-2 rounded-full ${modified ? 'bg-emerald-500' : 'bg-slate-300'}`}
                  aria-hidden="true"
                />
                <span
                  className={`text-[11px] font-semibold tracking-wide ${
                    modified ? 'text-amber-800' : 'text-slate-700'
                  }`}
                >
                  {APPLICATION_DIFF_COPY.currentSectionTitle}
                </span>
                <span
                  className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium ${
                    modified ? 'bg-emerald-100 text-emerald-700' : 'bg-white text-slate-500'
                  }`}
                >
                  {modified
                    ? APPLICATION_DIFF_COPY.currentDetectedBadge
                    : APPLICATION_DIFF_COPY.currentEditingBadge}
                </span>
              </div>
              {modified ? (
                <div className="mt-2">
                  <FieldDiffPreview originalValue={currentSavedValue} currentValue={currentEditingValue} />
                </div>
              ) : (
                <div className="mt-2 rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-500">
                  {APPLICATION_DIFF_COPY.currentNoDiffHint}
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
                {APPLICATION_DIFF_COPY.sourcePrefix}
                {sourceInfo.label}
              </span>
              <button
                type="button"
                onClick={onToggleSourceDetail}
                className="text-[11px] font-medium text-blue-600 hover:text-blue-700"
              >
                {showSourceDetail
                  ? APPLICATION_DIFF_COPY.collapseSourceButton
                  : APPLICATION_DIFF_COPY.viewSourceButton}
              </button>
            </div>
            {showSourceDetail ? (
              <div className="mt-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs leading-5 text-slate-600">
                {sourceInfo.detail}
              </div>
            ) : null}
          </div>
        )}
      </td>
    </tr>
  );
};

export default ApplicationFieldRow;
