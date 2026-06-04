import { spawnSync } from 'node:child_process';

import { buildInfoPath, prepareBuildInfo } from './versioning.mjs';

function runStep(command, args) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

const version = prepareBuildInfo();
console.log(`[build-info] prepared version=${version}`);
console.log(`[build-info] wrote ${buildInfoPath}`);

runStep('npx', ['tsc']);
runStep('npx', ['vite', 'build']);
