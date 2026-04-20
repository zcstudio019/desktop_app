export type ApplicationDiffFilterMode = 'all' | 'current' | 'history';

export interface ApplicationDiffStats {
  total: number;
  current: number;
  history: number;
}

export interface VisibleFieldCount {
  visible: number;
  total: number;
}

export interface ApplicationDiffTarget {
  rowKey: string;
  groupKey: string;
  label: string;
  shortLabel: string;
  kind: 'current' | 'history' | 'both';
  tooltip: string;
}

export function matchesDiffFilter(
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

export function hasVisibleFieldsInSection(
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

export function countDiffStats(
  applicationData: Record<string, Record<string, unknown>>,
  currentSavedData: Record<string, Record<string, unknown>>,
  previousSavedData: Record<string, Record<string, unknown>> | null,
): ApplicationDiffStats {
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

export function countVisibleFieldsInSection(
  sectionData: Record<string, unknown>,
  currentSavedData: Record<string, unknown>,
  previousSavedData: Record<string, unknown> | null,
  diffFilter: ApplicationDiffFilterMode,
): VisibleFieldCount {
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

export function fieldMatchesCurrentDiff(
  currentSavedValue: string,
  currentEditingValue: string,
): boolean {
  return currentSavedValue !== currentEditingValue;
}

export function fieldMatchesHistoryDiff(
  previousSavedValue: string,
  currentSavedValue: string,
): boolean {
  return previousSavedValue !== '' && previousSavedValue !== currentSavedValue;
}

export function collectDiffTargets(
  sectionPath: string,
  sectionData: Record<string, unknown>,
  currentSavedData: Record<string, unknown>,
  previousSavedData: Record<string, unknown> | null,
  diffFilter: ApplicationDiffFilterMode,
): ApplicationDiffTarget[] {
  const targets: ApplicationDiffTarget[] = [];

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
