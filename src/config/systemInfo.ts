import { BUILD_INFO } from '../generated/build-info';

export const SYSTEM_INFO = {
  name: '贷款助手',
  subtitle: '客户资料驱动的融资处理系统',
  baseVersion: BUILD_INFO.version,
  version: BUILD_INFO.version,
  releaseDate: BUILD_INFO.releaseDate,
  buildTime: BUILD_INFO.buildTime,
  commitHash: BUILD_INFO.commitHash,
} as const;

export function getSystemVersionLabel(): string {
  return `版本 ${SYSTEM_INFO.version}`;
}
