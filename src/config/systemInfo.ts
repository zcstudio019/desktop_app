import { BUILD_INFO } from '../generated/build-info';

export const SYSTEM_INFO = {
  name: '鏅鸿兘璐锋鍔╂墜',
  subtitle: '鏅鸿兘璐锋鐢宠绠＄悊绯荤粺',
  baseVersion: BUILD_INFO.version,
  version: BUILD_INFO.version,
  releaseDate: BUILD_INFO.releaseDate,
  buildTime: BUILD_INFO.buildTime,
  commitHash: BUILD_INFO.commitHash,
} as const;

export function getSystemVersionLabel(): string {
  return `绯荤粺鐗堟湰 ${SYSTEM_INFO.version}`;
}
