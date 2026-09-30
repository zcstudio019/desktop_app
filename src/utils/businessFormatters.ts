export function formatAmountWan(value: string | number | null | undefined): string {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '待确认';
  return `${(amount / 10_000).toLocaleString('zh-CN', { maximumFractionDigits: 2 })}万元`;
}

export function formatMonths(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === '') return '待确认';
  return `${value}个月`;
}

export function formatBusinessDateTime(value: string | null | undefined): string {
  if (!value) return '时间待确认';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '时间待确认' : date.toLocaleString('zh-CN', { hour12: false });
}
