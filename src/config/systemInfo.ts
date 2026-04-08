export const SYSTEM_INFO = {
  name: '贷款助手',
  subtitle: '智能贷款申请管理系统',
  version: 'V1.0.0',
  releaseDate: '2026/04/08',
} as const;

export function getSystemVersionLabel(): string {
  return `系统版本 ${SYSTEM_INFO.version}`;
}
