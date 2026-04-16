/**
 * Shared hooks entrypoint.
 *
 * Keep commonly reused state hooks exported from one place so feature pages
 * can compose conversation, task, and result-panel logic consistently.
 */

export { useLoading } from './useLoading';
export type { UseLoadingReturn } from './useLoading';

export { useAbortController } from './useAbortController';
export type { UseAbortControllerReturn } from './useAbortController';

export { useConversationState } from './useConversationState';
export type { ConversationStatus } from './useConversationState';

export { useTaskState } from './useTaskState';
export type { TaskViewSource } from './useTaskState';

export { useResultPanelState } from './useResultPanelState';
export type { ResultPanelMode } from './useResultPanelState';
