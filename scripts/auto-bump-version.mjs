import { bumpVersion } from './versioning.mjs';

const { currentVersion, nextVersion } = bumpVersion();
console.log(`[version] ${currentVersion} -> ${nextVersion}`);
