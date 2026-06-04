import { BUILD_INFO } from '../generated/build-info';

function normalizeVersion(version: string): string {
  return version.startsWith('V') ? version : `V${version}`;
}

export const APP_VERSION = normalizeVersion(BUILD_INFO.version);
