import path from 'node:path';

import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

import { bumpVersion, packageJsonPath, prepareVersionMeta, projectRoot, versionMetaPath } from './scripts/versioning.mjs';

function autoVersionPlugin() {
  let isUpdating = false;
  let pendingTimer = null;
  const ignoredPaths = new Set([
    path.normalize(packageJsonPath),
    path.normalize(versionMetaPath),
  ]);

  const shouldTrackFile = (file) => {
    const normalized = path.normalize(file);
    if (ignoredPaths.has(normalized)) {
      return false;
    }

    const relativePath = path.relative(projectRoot, normalized);
    if (!relativePath || relativePath.startsWith('..')) {
      return false;
    }

    if (
      relativePath.startsWith('node_modules') ||
      relativePath.startsWith('dist') ||
      relativePath.startsWith('.git') ||
      relativePath.startsWith('data')
    ) {
      return false;
    }

    return /\.(tsx?|jsx?|css|scss|json|py|md)$/i.test(relativePath);
  };

  const scheduleBump = (server, file) => {
    if (!shouldTrackFile(file) || isUpdating) {
      return;
    }

    if (pendingTimer) {
      clearTimeout(pendingTimer);
    }

    pendingTimer = setTimeout(() => {
      pendingTimer = null;
      isUpdating = true;
      try {
        const { currentVersion, nextVersion } = bumpVersion();
        server.config.logger.info(
          `[version-watch] ${currentVersion} -> ${nextVersion} (${path.relative(projectRoot, file)})`,
        );
        server.ws.send({ type: 'full-reload' });
      } catch (error) {
        server.config.logger.error(`[version-watch] failed to bump version: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        isUpdating = false;
      }
    }, 150);
  };

  return {
    name: 'auto-version-watch',
    configResolved() {
      prepareVersionMeta();
    },
    configureServer(server) {
      const watchTargets = [
        path.resolve(projectRoot, 'src'),
        path.resolve(projectRoot, 'backend'),
      ];

      server.watcher.add(watchTargets);
      server.watcher.on('change', (file) => scheduleBump(server, file));
      server.watcher.on('add', (file) => scheduleBump(server, file));
      server.watcher.on('unlink', (file) => scheduleBump(server, file));
    },
  };
}

export default defineConfig({
  plugins: [react(), autoVersionPlugin()],
});
