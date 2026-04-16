import { useMemo, useState } from 'react';
import type { ChatJobSummaryResponse } from '../services/types';

export type TaskViewSource = 'none' | 'manual' | 'auto';

export function useTaskState(taskFeedback: unknown) {
  const [recentChatJobs, setRecentChatJobs] = useState<ChatJobSummaryResponse[]>([]);
  const [jobsLoading, setJobsLoading] = useState(false);
  const [jobFilterMode, setJobFilterMode] = useState<'current' | 'all'>('current');
  const [showRecentJobs, setShowRecentJobs] = useState(true);
  const [collapsedJobGroups, setCollapsedJobGroups] = useState<Record<string, boolean>>({
    running: false,
    success: true,
    failed: false,
  });
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [activeJobSource, setActiveJobSource] = useState<TaskViewSource>('none');
  const [currentRunningJobId, setCurrentRunningJobId] = useState<string | null>(null);

  const taskLayer = useMemo(
    () => ({
      jobList: recentChatJobs,
      activeJobId,
      activeJobSource,
      pollingJobIds: currentRunningJobId ? [currentRunningJobId] : [],
      taskFeedback,
    }),
    [recentChatJobs, activeJobId, activeJobSource, currentRunningJobId, taskFeedback],
  );

  return {
    recentChatJobs,
    setRecentChatJobs,
    jobsLoading,
    setJobsLoading,
    jobFilterMode,
    setJobFilterMode,
    showRecentJobs,
    setShowRecentJobs,
    collapsedJobGroups,
    setCollapsedJobGroups,
    activeJobId,
    setActiveJobId,
    activeJobSource,
    setActiveJobSource,
    currentRunningJobId,
    setCurrentRunningJobId,
    taskLayer,
  };
}
