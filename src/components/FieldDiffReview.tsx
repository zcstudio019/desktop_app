import React from 'react';

import FieldDiffPreview from './FieldDiffPreview';

interface FieldDiffReviewProps {
  title: string;
  originalValue: string | undefined | null;
  currentValue: string | undefined | null;
  badgeLabel?: string;
}

const FieldDiffReview: React.FC<FieldDiffReviewProps> = ({
  title,
  originalValue,
  currentValue,
  badgeLabel,
}) => {
  if ((originalValue ?? '') === (currentValue ?? '')) {
    return null;
  }

  return (
    <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 px-2.5 py-2">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <span className="text-[11px] font-semibold tracking-wide text-slate-600">{title}</span>
        {badgeLabel ? (
          <span className="inline-flex items-center rounded-full bg-white px-2 py-0.5 text-[11px] font-medium text-slate-500">
            {badgeLabel}
          </span>
        ) : null}
      </div>
      <FieldDiffPreview originalValue={originalValue} currentValue={currentValue} />
    </div>
  );
};

export default FieldDiffReview;
