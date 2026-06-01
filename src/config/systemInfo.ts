import { BUILD_INFO } from '../generated/build-info';
import { BRAND } from './brand';

export const SYSTEM_INFO = {
  name: BRAND.appName,
  subtitle: BRAND.subtitle,
  baseVersion: BUILD_INFO.version,
  version: BUILD_INFO.version,
  releaseDate: BUILD_INFO.releaseDate,
  buildTime: BUILD_INFO.buildTime,
  commitHash: BUILD_INFO.commitHash,
} as const;

export function getSystemVersionLabel(): string {
  return `版本 ${SYSTEM_INFO.version}`;
}
