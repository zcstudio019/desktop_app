import React from 'react';
import type { KycExtractionResult as KycExtractionResultType } from '../services/types';
import {
  formatKycDisplayValue,
  getKycDisplayEntries,
  getKycFieldLabel,
  isKycDocType,
} from '../utils/kycDisplayFields';

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

export function isKycExtractionResult(value: unknown): value is KycExtractionResultType {
  if (!isRecord(value)) return false;
  return value.agent_type === 'kyc_document_agent' || isKycDocType(value.doc_type);
}

export function renderKycDisplayMarkdown(fields: Array<[string, string]>): string {
  const lines = ['### 关键字段'];
  if (fields.length) {
    fields.forEach(([field, value]) => {
      lines.push(`- ${getKycFieldLabel(field)}: ${value}`);
    });
  } else {
    lines.push('- 暂无可展示字段');
  }
  return lines.join('\n');
}

export function enrichPropertyFieldsForDisplay(result: KycExtractionResultType): Record<string, unknown> {
  const rawFields = isRecord(result.fields) ? result.fields : {};
  const fields: Record<string, unknown> = { ...rawFields };
  const isPropertyCert = result.doc_type === 'property_cert' || result.doc_type === 'real_estate_cert';
  if (!isPropertyCert) return fields;

  const hasHouseUse = Boolean(fields['房屋用途'] || fields.house_use || fields.building_use || fields.use_type);
  if (hasHouseUse) return fields;

  const hasBuildingContext = Boolean(
    fields['室号或部位'] ||
      fields.room_number ||
      fields['建筑面积'] ||
      fields.building_area ||
      fields['建筑类型'] ||
      fields.building_type ||
      fields['总层数'] ||
      fields.total_floors ||
      fields['竣工日期'] ||
      fields.completion_date,
  );
  const { markdown, raw_text_preview: rawTextPreview } = result as { markdown?: unknown; raw_text_preview?: unknown };
  const sourceText = [
    typeof rawTextPreview === 'string' ? rawTextPreview : '',
    typeof markdown === 'string' ? markdown : '',
    result.evidence ? JSON.stringify(result.evidence) : '',
  ]
    .filter(Boolean)
    .join('\n');

  if (hasBuildingContext && sourceText.includes('居住')) {
    fields['房屋用途'] = '居住';
    fields.house_use = '居住';
    fields.building_use = '居住';
  }

  return fields;
}

interface Props {
  result: KycExtractionResultType;
}

const KycExtractionResult: React.FC<Props> = ({ result }) => {
  const fields = getKycDisplayEntries(enrichPropertyFieldsForDisplay(result), result.doc_type);
  const displayMarkdown = renderKycDisplayMarkdown(fields);
  const warnings = result.validation?.warnings || [];
  const errors = result.validation?.errors || [];
  const displayFieldNames = new Set(fields.map(([field]) => field));
  const evidence = Object.entries(result.evidence || {})
    .map(([field, item]) => [getKycFieldLabel(field), item] as const)
    .filter(([field]) => displayFieldNames.has(field))
    .filter(([field], index, entries) => entries.findIndex(([name]) => name === field) === index)
    .slice(0, 6);
  const statusLabel = result.extraction_status === 'success' ? '提取成功' : result.extraction_status === 'partial' ? '部分提取' : '提取失败';
  const title = result.doc_type === 'property_cert'
    ? '房产证/房地产权证'
    : result.doc_type_name || 'KYC资料';

  return (
    <div className="space-y-4 text-sm text-slate-700">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-base font-semibold text-slate-900">{title}</div>
          <div className="mt-1 text-xs text-slate-500">资料类型编码：{result.doc_type || 'unknown'}</div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">{statusLabel}</span>
          <span className="rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700">
            置信度：{typeof result.confidence?.overall === 'number' ? `${Math.round(result.confidence.overall * 100)}%` : '未计算'}
          </span>
        </div>
      </div>

      <div>
        <div className="mb-2 font-medium text-slate-900">关键字段</div>
        {fields.length > 0 ? (
          <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
            {fields.map(([field, value]) => (
              <div key={field} className="rounded-md border border-slate-200 bg-white px-3 py-2">
                <div className="break-words font-medium text-slate-900">
                  <span className="text-slate-500">{getKycFieldLabel(field)}: </span>{value}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="rounded-md bg-slate-50 px-3 py-2 text-slate-500">暂无可展示字段</div>
        )}
      </div>

      {(result.missing_fields || []).length > 0 && (
        <div>
          <div className="mb-2 font-medium text-slate-900">缺失字段</div>
          <div className="flex flex-wrap gap-2">
            {(result.missing_fields || []).map((field) => (
              <span key={field} className="rounded-full bg-amber-50 px-2.5 py-1 text-xs text-amber-700">
                {getKycFieldLabel(field)}
              </span>
            ))}
          </div>
        </div>
      )}

      {(warnings.length > 0 || errors.length > 0) && (
        <div>
          <div className="mb-2 font-medium text-slate-900">校验提醒</div>
          <div className="space-y-2">
            {errors.map((item) => (
              <div key={item} className="rounded-md bg-red-50 px-3 py-2 text-red-700">{item}</div>
            ))}
            {warnings.map((item) => (
              <div key={item} className="rounded-md bg-amber-50 px-3 py-2 text-amber-700">{item}</div>
            ))}
          </div>
        </div>
      )}

      {evidence.length > 0 && (
        <div>
          <div className="mb-2 font-medium text-slate-900">证据摘要</div>
          <div className="space-y-2">
            {evidence.map(([field, item]) => {
              const record = isRecord(item) ? item : {};
              return (
                <div key={field} className="rounded-md bg-slate-50 px-3 py-2">
                  <span className="font-medium text-slate-700">{getKycFieldLabel(field)}: </span>
                  <span className="text-slate-600">{formatKycDisplayValue(record.evidence_text || record.value || '')}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {displayMarkdown ? (
        <div>
          <div className="mb-2 font-medium text-slate-900">展示预览</div>
          <pre className="max-h-56 overflow-auto whitespace-pre-wrap rounded-md bg-slate-900 p-3 text-xs leading-5 text-slate-100">
            {displayMarkdown}
          </pre>
        </div>
      ) : null}
    </div>
  );
};

export default KycExtractionResult;
