import React, { useMemo } from 'react';

type DiffSegment = {
  type: 'equal' | 'removed' | 'added';
  value: string;
};

interface FieldDiffPreviewProps {
  originalValue: string | undefined | null;
  currentValue: string | undefined | null;
}

function normalizeValue(value: string | undefined | null): string {
  return (value ?? '').replace(/\r\n/g, '\n');
}

function buildDiffSegments(originalValue: string, currentValue: string): DiffSegment[] {
  const before = Array.from(normalizeValue(originalValue));
  const after = Array.from(normalizeValue(currentValue));

  if (before.join('') === after.join('')) {
    return [];
  }

  const rows = before.length;
  const cols = after.length;
  const lcs: number[][] = Array.from({ length: rows + 1 }, () => Array(cols + 1).fill(0));

  for (let row = rows - 1; row >= 0; row -= 1) {
    for (let col = cols - 1; col >= 0; col -= 1) {
      if (before[row] === after[col]) {
        lcs[row][col] = lcs[row + 1][col + 1] + 1;
      } else {
        lcs[row][col] = Math.max(lcs[row + 1][col], lcs[row][col + 1]);
      }
    }
  }

  const segments: DiffSegment[] = [];
  const pushSegment = (type: DiffSegment['type'], value: string) => {
    if (!value) return;
    const last = segments[segments.length - 1];
    if (last && last.type === type) {
      last.value += value;
      return;
    }
    segments.push({ type, value });
  };

  let row = 0;
  let col = 0;

  while (row < rows && col < cols) {
    if (before[row] === after[col]) {
      pushSegment('equal', before[row]);
      row += 1;
      col += 1;
    } else if (lcs[row + 1][col] >= lcs[row][col + 1]) {
      pushSegment('removed', before[row]);
      row += 1;
    } else {
      pushSegment('added', after[col]);
      col += 1;
    }
  }

  while (row < rows) {
    pushSegment('removed', before[row]);
    row += 1;
  }

  while (col < cols) {
    pushSegment('added', after[col]);
    col += 1;
  }

  return segments;
}

const FieldDiffPreview: React.FC<FieldDiffPreviewProps> = ({
  originalValue,
  currentValue,
}) => {
  const original = normalizeValue(originalValue);
  const current = normalizeValue(currentValue);

  const diffSegments = useMemo(
    () => buildDiffSegments(original, current),
    [original, current],
  );

  if (diffSegments.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-slate-200 bg-white px-2.5 py-2">
      <div className="mb-1 text-[11px] font-semibold tracking-wide text-slate-500">实时差异预览</div>
      <div className="whitespace-pre-wrap break-words leading-6">
        {diffSegments.map((segment, index) => {
          if (segment.type === 'equal') {
            return (
              <span key={`diff-${index}`} className="text-slate-500">
                {segment.value}
              </span>
            );
          }

          if (segment.type === 'removed') {
            return (
              <span
                key={`diff-${index}`}
                className="rounded bg-rose-50 px-0.5 text-rose-700 line-through"
              >
                {segment.value}
              </span>
            );
          }

          return (
            <span
              key={`diff-${index}`}
              className="rounded bg-emerald-100 px-0.5 font-medium text-emerald-700"
            >
              {segment.value}
            </span>
          );
        })}
      </div>
      <div className="mt-2 flex flex-wrap gap-2 text-[11px]">
        <span className="inline-flex items-center rounded-full bg-rose-50 px-2 py-0.5 text-rose-700">红色删除线 = 已删除</span>
        <span className="inline-flex items-center rounded-full bg-emerald-100 px-2 py-0.5 text-emerald-700">绿色高亮 = 新增内容</span>
      </div>
    </div>
  );
};

export default FieldDiffPreview;
