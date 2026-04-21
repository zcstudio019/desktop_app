export type ApplicationDiffCatalogFilterMode = 'all' | 'current' | 'history' | 'both';

export const APPLICATION_DIFF_COPY = {
  historySectionTitle: '上一版变更记录',
  historyDetectedBadge: '检测到历史差异',
  historyExpandButton: '查看上一版差异',
  historyCollapseButton: '收起上一版差异',
  historyCollapsedHint: '已检测到上一保存版本差异，点击“查看上一版差异”可展开详情。',
  currentSectionTitle: '本次编辑差异',
  currentDetectedBadge: '已检测到本次修改',
  currentEditingBadge: '编辑中',
  currentNoDiffHint: '本次编辑暂无差异。',
  sourcePrefix: '来源：',
  viewSourceButton: '查看来源',
  collapseSourceButton: '收起来源',
  catalogTitle: '差异目录',
  catalogSubtitleDefault: '右侧审阅面板会汇总当前字段差异，点击目录项可直接定位。',
  catalogSubtitleCurrentOnly: '当前面板聚焦本次编辑差异，适合快速核对你刚刚改过的字段。',
  catalogSubtitleHistoryOnly: '当前面板聚焦历史差异，适合回看这一版相对上一版的变化。',
  catalogSubtitleMixed: '当前面板同时包含本次编辑和历史差异，可在目录中快速切换两类变化。',
  catalogEmptyState: '当前目录筛选下没有命中的差异字段，可以切换其他目录筛选查看。',
  activeTargetTitle: '当前定位字段',
  groupItemSuffix: '项',
  groupCurrentLabel: '本次修改',
  groupHistoryLabel: '历史差异',
  groupBothLabel: '双重差异',
  catalogFilters: {
    all: '全部差异',
    current: '仅本次修改',
    history: '仅历史差异',
    both: '仅双重差异',
  } satisfies Record<ApplicationDiffCatalogFilterMode, string>,
  diffKindLabels: {
    current: '本次修改',
    history: '历史差异',
    both: '双重差异',
  } as const,
} as const;

export const APPLICATION_RESULT_COPY = {
  unnamedCustomer: '未命名客户',
  cardTitleGenerated: '申请表已生成',
  cardTitleBlank: '空白申请表模板',
  editModeBadge: '编辑中',
  customerLabel: '客户：',
  generatedAtLabel: '生成时间：',
  profileVersionLabel: '资料汇总版本：',
  profileUpdatedAtLabel: '资料更新时间：',
  saveButtonIdle: '保存',
  saveButtonSaving: '保存中',
  saveButtonDisabled: '无修改可保存',
  saveButtonShortcutHint: 'Ctrl/⌘ + S',
  cancelEditWithChanges: '放弃修改',
  cancelEditWithoutChanges: '退出编辑',
  cancelEditShortcutHint: 'Esc',
  editButton: '编辑',
  downloadButton: '下载表单',
  downloadDraftButton: '下载当前编辑稿',
  collapseCardButton: '收起申请表结果',
  expandCardButton: '展开申请表结果',
  editSummaryPrefix: '编辑摘要',
  editSummaryStats: (current: number, history: number, total: number) =>
    `已改 ${current} 项 · 历史 ${history} 项 · 共 ${total} 项`,
  editSummaryShortcutHint: '快捷键：Ctrl/⌘ + S 保存 · Esc 退出',
  unsavedChangesBadge: '当前有未保存修改',
  toggleCatalogOpen: '打开差异目录',
  toggleCatalogClose: '收起差异目录',
  resetReviewViewButton: '恢复默认视图',
  filterGroupTitle: '筛选视图',
  filterAllFields: '全部字段',
  filterCurrentDiff: (count: number) => `仅看本次修改${count > 0 ? ` (${count})` : ''}`,
  filterHistoryDiff: (count: number) => `仅看历史差异${count > 0 ? ` (${count})` : ''}`,
  bulkActionsTitle: '批量操作',
  bulkActionsEmptyHistory: '当前没有可批量展开的历史差异',
  expandAllHistory: '展开全部历史差异',
  collapseAllHistory: '收起全部历史差异',
  expandAllSections: '展开全部分组',
  collapseAllSections: '收起全部分组',
  navigationTitle: '差异导航',
  navigationEmptyState: '当前筛选下没有可跳转的差异字段',
  previousDiffField: '上一个差异字段',
  nextDiffField: '下一个差异字段',
  discardChangesConfirm: '当前申请表还有未保存的修改，确定要退出编辑并放弃这些改动吗？',
  saveVersionWithNo: (versionNo: number) => `已保存为当前版本 V${versionNo}`,
  saveVersionGeneric: '已保存为当前版本',
  saveHistoryReady: '已保留上一版本差异供对比。',
  saveHistoryEmpty: '当前还没有可对比的上一版本。',
  saveFailedFallback: '保存申请表失败，请稍后重试。',
  downloadFilePrefix: '贷款申请表',
  staleTitle: '这份申请表已被新上传资料覆盖',
  staleFallbackTime: '请重新生成后再用于方案匹配或后续沟通。',
  regenerateButton: '去申请表页重新生成',
  emptyFilteredState: '当前筛选下没有命中的字段，请切换筛选后再查看。',
} as const;

export function getCatalogFilterLabel(
  mode: ApplicationDiffCatalogFilterMode,
  count: number,
): string {
  return `${APPLICATION_DIFF_COPY.catalogFilters[mode]} (${count})`;
}

export function getSectionCountLabel(
  visible: number,
  total: number,
  filtered: boolean,
): string {
  return filtered ? `(${visible}/${total} 项)` : `(${total} 项)`;
}

export function getDiffKindLabel(kind: 'current' | 'history' | 'both'): string {
  return APPLICATION_DIFF_COPY.diffKindLabels[kind];
}

export function getCatalogSubtitle(counts: {
  currentCount: number;
  historyCount: number;
  bothCount: number;
}): string {
  const { currentCount, historyCount, bothCount } = counts;

  if (bothCount > 0 || (currentCount > 0 && historyCount > 0)) {
    return APPLICATION_DIFF_COPY.catalogSubtitleMixed;
  }

  if (currentCount > 0) {
    return APPLICATION_DIFF_COPY.catalogSubtitleCurrentOnly;
  }

  if (historyCount > 0) {
    return APPLICATION_DIFF_COPY.catalogSubtitleHistoryOnly;
  }

  return APPLICATION_DIFF_COPY.catalogSubtitleDefault;
}
