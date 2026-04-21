import React from 'react';
import type { ApplicationDiffTarget } from './applicationDiffUtils';
import {
  APPLICATION_DIFF_COPY,
  getCatalogSubtitle,
  getCatalogFilterLabel,
  getDiffKindLabel,
} from './applicationDiffCopy';

export type ApplicationDiffCatalogFilterMode = 'all' | 'current' | 'history' | 'both';

export interface ApplicationDiffTargetGroup {
  groupKey: string;
  count: number;
  currentCount: number;
  historyCount: number;
  bothCount: number;
  items: ApplicationDiffTarget[];
}

interface ApplicationDiffCatalogPanelProps {
  diffTargets: ApplicationDiffTarget[];
  filteredGroupedDiffTargets: ApplicationDiffTargetGroup[];
  activeDiffTarget: ApplicationDiffTarget | null;
  activeDiffRowKey: string | null;
  diffCatalogFilter: ApplicationDiffCatalogFilterMode;
  onChangeCatalogFilter: (value: ApplicationDiffCatalogFilterMode) => void;
  onJumpToField: (rowKey: string) => void;
}

const ApplicationDiffCatalogPanel: React.FC<ApplicationDiffCatalogPanelProps> = ({
  diffTargets,
  filteredGroupedDiffTargets,
  activeDiffTarget,
  activeDiffRowKey,
  diffCatalogFilter,
  onChangeCatalogFilter,
  onJumpToField,
}) => {
  const currentCount = diffTargets.filter((item) => item.kind === 'current').length;
  const historyCount = diffTargets.filter((item) => item.kind === 'history').length;
  const bothCount = diffTargets.filter((item) => item.kind === 'both').length;
  const catalogSubtitle = getCatalogSubtitle({ currentCount, historyCount, bothCount });

  return (
    <aside className="2xl:sticky 2xl:top-4 2xl:self-start">
      <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 xl:px-2.5 2xl:px-3">
        <div className="mb-2 flex flex-col gap-1">
          <div>
            <div className="text-xs font-semibold tracking-wide text-slate-700">
              {APPLICATION_DIFF_COPY.catalogTitle}
            </div>
            <div className="mt-1 text-[11px] leading-5 text-slate-400">
              {catalogSubtitle}
            </div>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {([
              { value: 'all', count: diffTargets.length },
              { value: 'current', count: currentCount },
              { value: 'history', count: historyCount },
              { value: 'both', count: bothCount },
            ] as const).map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => onChangeCatalogFilter(option.value)}
                aria-pressed={diffCatalogFilter === option.value}
                title={getCatalogFilterLabel(option.value, option.count)}
                className={`rounded-full border px-2.5 py-1.5 text-[11px] font-medium transition-colors ${
                  diffCatalogFilter === option.value
                    ? 'border-blue-500 bg-blue-50 text-blue-700'
                    : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-800'
                }`}
              >
                {getCatalogFilterLabel(option.value, option.count)}
              </button>
            ))}
          </div>
        </div>

        {filteredGroupedDiffTargets.length === 0 ? (
          <div className="rounded-lg border border-dashed border-slate-300 bg-white px-4 py-4 text-sm text-slate-500">
            {APPLICATION_DIFF_COPY.catalogEmptyState}
          </div>
        ) : (
          <div className="max-h-[60vh] space-y-2.5 overflow-y-auto pr-1">
            {activeDiffTarget ? (
              <div className="rounded-lg border border-blue-100 bg-blue-50/60 px-3 py-2">
                <div className="mb-1 flex flex-wrap items-center gap-2">
                  <span
                    className={`inline-flex h-2 w-2 rounded-full ${
                      activeDiffTarget.kind === 'both'
                        ? 'bg-violet-500'
                        : activeDiffTarget.kind === 'history'
                          ? 'bg-amber-400'
                          : 'bg-emerald-500'
                    }`}
                    aria-hidden="true"
                  />
                  <div className="text-[11px] font-semibold text-blue-800">
                    {APPLICATION_DIFF_COPY.activeTargetTitle}
                  </div>
                  <span className="inline-flex items-center rounded-full bg-white px-2 py-0.5 text-[11px] font-medium text-blue-700">
                    {getDiffKindLabel(activeDiffTarget.kind)}
                  </span>
                </div>
                <div className="text-xs font-medium leading-5 text-slate-800">{activeDiffTarget.label}</div>
                <div className="mt-1.5 whitespace-pre-line rounded-lg border border-blue-100 bg-white px-3 py-2 text-[11px] leading-5 text-slate-500">
                  {activeDiffTarget.tooltip}
                </div>
              </div>
            ) : null}

            {filteredGroupedDiffTargets.map((group) => (
              <div key={group.groupKey} className="rounded-lg border border-slate-200 bg-white px-3 py-3">
                <div className="mb-2 flex flex-col gap-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-xs font-semibold text-slate-700">{group.groupKey}</span>
                    <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                      {group.items.length} {APPLICATION_DIFF_COPY.groupItemSuffix}
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-2 text-[11px] text-slate-400">
                    {group.currentCount > 0 ? (
                      <span>
                        {APPLICATION_DIFF_COPY.groupCurrentLabel} {group.currentCount}
                      </span>
                    ) : null}
                    {group.historyCount > 0 ? (
                      <span>
                        {APPLICATION_DIFF_COPY.groupHistoryLabel} {group.historyCount}
                      </span>
                    ) : null}
                    {group.bothCount > 0 ? (
                      <span>
                        {APPLICATION_DIFF_COPY.groupBothLabel} {group.bothCount}
                      </span>
                    ) : null}
                  </div>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {group.items.map((target) => (
                    <button
                      key={target.rowKey}
                      type="button"
                      onClick={() => onJumpToField(target.rowKey)}
                      aria-label={`定位到字段：${target.label}`}
                      title={target.tooltip}
                      className={`inline-flex max-w-full items-center gap-2 rounded-full border px-2.5 py-1.5 text-xs font-medium transition-colors ${
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
                      <span className="max-w-[12rem] truncate" title={target.label}>
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
    </aside>
  );
};

export default ApplicationDiffCatalogPanel;
