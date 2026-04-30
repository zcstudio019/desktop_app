import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const projectRoot = path.resolve(__dirname, '..');
export const packageJsonPath = path.resolve(projectRoot, 'package.json');
export const versionMetaPath = path.resolve(projectRoot, 'src', 'generated', 'version.ts');
export const buildInfoPath = path.resolve(projectRoot, 'src', 'generated', 'build-info.ts');
export const versionHistoryPath = path.resolve(projectRoot, 'logs', 'version-history.log');

function pad(value) {
  return String(value).padStart(2, '0');
}

function formatDate(date) {
  return `${date.getFullYear()}/${pad(date.getMonth() + 1)}/${pad(date.getDate())}`;
}

function formatDateTime(date) {
  return `${formatDate(date)} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function escapeTsString(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

export function incrementVersion(version) {
  const parts = version.split('.').map((item) => Number.parseInt(item, 10));
  if (parts.length !== 3 || parts.some((item) => Number.isNaN(item) || item < 0)) {
    throw new Error(`Unsupported version format: ${version}`);
  }

  let [major, minor, patch] = parts;
  patch += 1;

  if (patch > 9) {
    patch = 0;
    minor += 1;
  }

  if (minor > 9) {
    minor = 0;
    major += 1;
  }

  return `${major}.${minor}.${patch}`;
}

export function readPackageJson() {
  return JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
}

export function getCurrentVersion() {
  const packageJson = readPackageJson();
  return packageJson.version || '1.0.0';
}

export function writePackageVersion(version) {
  const packageJson = readPackageJson();
  packageJson.version = version;
  fs.writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`, 'utf8');
  return packageJson;
}

export function writeVersionMeta(version, now = new Date()) {
  const versionMetaContent = `export const VERSION_META = {
  version: "V${version}",
  releaseDate: "${formatDate(now)}",
  buildTime: "${formatDateTime(now)}",
} as const;
`;

  fs.mkdirSync(path.dirname(versionMetaPath), { recursive: true });
  fs.writeFileSync(versionMetaPath, versionMetaContent, 'utf8');
}

export function getGitShortHash() {
  const result = spawnSync('git', ['rev-parse', '--short', 'HEAD'], {
    cwd: projectRoot,
    encoding: 'utf8',
    shell: process.platform === 'win32',
  });

  if (result.status !== 0) {
    return 'unknown';
  }

  return String(result.stdout || '').trim() || 'unknown';
}

export function writeBuildInfo(version, now = new Date(), commitHash = getGitShortHash()) {
  const buildInfoContent = `export const BUILD_INFO = {
  version: "V${escapeTsString(version)}",
  releaseDate: "${escapeTsString(formatDate(now))}",
  buildTime: "${escapeTsString(formatDateTime(now))}",
  commitHash: "${escapeTsString(commitHash)}",
} as const;
`;

  fs.mkdirSync(path.dirname(buildInfoPath), { recursive: true });
  fs.writeFileSync(buildInfoPath, buildInfoContent, 'utf8');
}

export function appendVersionHistory(currentVersion, nextVersion, now = new Date()) {
  const historyLine = `[${formatDateTime(now)}] ${currentVersion} -> ${nextVersion}\n`;
  fs.mkdirSync(path.dirname(versionHistoryPath), { recursive: true });
  fs.appendFileSync(versionHistoryPath, historyLine, 'utf8');
}

export function prepareVersionMeta() {
  const version = getCurrentVersion();
  writeVersionMeta(version);
  writeBuildInfo(version);
  return version;
}

export function prepareBuildInfo() {
  const version = getCurrentVersion();
  writeBuildInfo(version);
  return version;
}

export function bumpVersion() {
  const now = new Date();
  const currentVersion = getCurrentVersion();
  const nextVersion = incrementVersion(currentVersion);
  writePackageVersion(nextVersion);
  writeVersionMeta(nextVersion, now);
  writeBuildInfo(nextVersion, now);
  appendVersionHistory(currentVersion, nextVersion, now);
  return {
    currentVersion,
    nextVersion,
  };
}
