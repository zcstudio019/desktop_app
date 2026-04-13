import { prepareVersionMeta, versionMetaPath } from './versioning.mjs';

prepareVersionMeta();
console.log(`[version-meta] wrote ${versionMetaPath}`);
