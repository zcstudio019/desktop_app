import { VERSION_META } from '../generated/version';

export const SYSTEM_INFO = {
  name: '智能贷款助手',
  subtitle: '智能贷款申请管理系统',
  baseVersion: VERSION_META.version,
  version: VERSION_META.version,
  releaseDate: VERSION_META.releaseDate,
  buildTime: VERSION_META.buildTime,
} as const;

export function getSystemVersionLabel(): string {
  return `系统版本 ${SYSTEM_INFO.version}`;
}
