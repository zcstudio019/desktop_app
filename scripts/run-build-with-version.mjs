import { spawnSync } from 'node:child_process';

import { bumpVersion } from './versioning.mjs';

function runStep(command, args) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });

  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

const { currentVersion, nextVersion } = bumpVersion();
console.log(`[build-version] ${currentVersion} -> ${nextVersion}`);

runStep('npx', ['tsc']);
runStep('npx', ['vite', 'build']);
