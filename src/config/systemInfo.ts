import packageJson from '../../package.json';

function pad(value: number) {
  return String(value).padStart(2, '0');
}

function formatBuildDate(value: Date) {
  return `${value.getFullYear()}/${pad(value.getMonth() + 1)}/${pad(value.getDate())}`;
}

function formatBuildDateTime(value: Date) {
  return `${formatBuildDate(value)} ${pad(value.getHours())}:${pad(value.getMinutes())}:${pad(value.getSeconds())}`;
}

const buildTime = new Date();
const version = `V${packageJson.version}`;

export const SYSTEM_INFO = {
  name: '贷款助手',
  subtitle: '智能贷款申请管理系统',
  baseVersion: version,
  version,
  releaseDate: formatBuildDate(buildTime),
  buildTime: formatBuildDateTime(buildTime),
} as const;

export function getSystemVersionLabel(): string {
  return `系统版本 ${SYSTEM_INFO.version}`;
}
