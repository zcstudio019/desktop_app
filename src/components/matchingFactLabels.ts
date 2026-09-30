import { RULE_FIELD_LABELS } from './admin/productCatalogLabels';

export const MATCHING_FACT_LABELS: Record<string, string> = {
  ...RULE_FIELD_LABELS,
  'cashflow.operating_inflow': '经营流入',
  'cashflow.non_operating_or_unidentified_inflow': '其他/未识别流入',
  'cashflow.average_monthly_operating_inflow': '月均经营流入',
};

export function matchingFactLabel(fieldPath: string): string {
  const label = MATCHING_FACT_LABELS[fieldPath];
  if (label) return label;
  console.warn(`[ProductMatching] Missing matching fact label: ${fieldPath}`);
  return '未命名字段';
}

