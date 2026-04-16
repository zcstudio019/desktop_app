import { useMemo, useState } from 'react';
import type { ChatJobSummaryResponse } from '../services/types';

export type ResultPanelMode = 'empty' | 'loading' | 'task_result';

export function useResultPanelState() {
  const [currentJob, setCurrentJob] = useState<ChatJobSummaryResponse | null>(null);
  const [resultPanelMode, setResultPanelMode] = useState<ResultPanelMode>('empty');
  const [activeResultJobId, setActiveResultJobId] = useState<string | null>(null);
  const [activeResultData, setActiveResultData] = useState<Record<string, unknown> | null>(null);

  const resultLayer = useMemo(
    () => ({
      resultPanelMode,
      activeResultJobId,
      activeResultData,
    }),
    [resultPanelMode, activeResultJobId, activeResultData],
  );

  return {
    currentJob,
    setCurrentJob,
    resultPanelMode,
    setResultPanelMode,
    activeResultJobId,
    setActiveResultJobId,
    activeResultData,
    setActiveResultData,
    resultLayer,
  };
}
