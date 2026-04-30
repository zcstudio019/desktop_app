import { buildInfoPath, prepareBuildInfo } from './versioning.mjs';

const version = prepareBuildInfo();
console.log(`[build-info] prepared version=${version}`);
console.log(`[build-info] wrote ${buildInfoPath}`);
