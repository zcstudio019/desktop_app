import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const packageJsonPath = path.resolve(__dirname, '..', 'package.json');
const versionMetaPath = path.resolve(__dirname, '..', 'src', 'generated', 'version.ts');

function pad(value) {
  return String(value).padStart(2, '0');
}

function formatDate(date) {
  return `${date.getFullYear()}/${pad(date.getMonth() + 1)}/${pad(date.getDate())}`;
}

function formatDateTime(date) {
  return `${formatDate(date)} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

const packageJsonRaw = fs.readFileSync(packageJsonPath, 'utf8');
const packageVersionMatch = packageJsonRaw.match(/"version"\s*:\s*"([^"]+)"/);
const packageVersion = packageVersionMatch?.[1] || '1.0.0';
const now = new Date();

const versionMetaContent = `export const VERSION_META = {
  version: "V${packageVersion}",
  releaseDate: "${formatDate(now)}",
  buildTime: "${formatDateTime(now)}",
} as const;
`;

fs.mkdirSync(path.dirname(versionMetaPath), { recursive: true });
fs.writeFileSync(versionMetaPath, versionMetaContent, 'utf8');

console.log(`[version-meta] wrote ${versionMetaPath}`);
