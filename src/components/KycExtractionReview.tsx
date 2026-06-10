import React, { useEffect, useMemo, useState } from 'react';
import type { ExtractionReviewResult } from '../services/types';
import { BUSINESS_LICENSE_FIELD_ORDER, formatKycDisplayValue, getKycDisplayEntries, getKycFieldLabel } from '../utils/kycDisplayFields';

function valueText(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') {
    if (Array.isArray(value)) return value.join('、');
    const record = value as Record<string, unknown>;
    if ('amount' in record && 'unit' in record) return formatKycDisplayValue(value);
    if ('value' in record && 'unit' in record) return formatKycDisplayValue(value);
    return formatKycDisplayValue(value);
  }
  return String(value);
}

interface Props {
  review: ExtractionReviewResult;
  canEdit: boolean;
  saving?: boolean;
  onCancel: () => void;
  onSave: (fields: Record<string, unknown>, status: 'partial' | 'confirmed') => void;
}

const KycExtractionReview: React.FC<Props> = ({ review, canEdit, saving, onCancel, onSave }) => {
  const extractedFields = useMemo(() => {
    const fields = review.extracted_data?.fields;
    return fields && typeof fields === 'object' && !Array.isArray(fields) ? fields as Record<string, unknown> : {};
  }, [review.extracted_data]);
  const confirmedFields = review.confirmed_data?.confirmed_fields || {};
  const mergedFields = useMemo(
    () => ({ ...extractedFields, ...(review.merged_fields || {}), ...confirmedFields }),
    [confirmedFields, extractedFields, review.merged_fields],
  );
  const displayEntries = useMemo(() => {
    return getKycDisplayEntries(mergedFields, review.doc_type);
  }, [mergedFields, review.doc_type]);
  const fieldKeys = useMemo(
    () => (review.doc_type === 'business_license' ? BUSINESS_LICENSE_FIELD_ORDER : displayEntries.map(([field]) => field)),
    [displayEntries, review.doc_type],
  );
  const displayFieldMap = useMemo(() => {
    if (review.doc_type === 'business_license') {
      return Object.fromEntries(BUSINESS_LICENSE_FIELD_ORDER.map((field) => [field, valueText(mergedFields[field])]));
    }
    return Object.fromEntries(displayEntries);
  }, [displayEntries, mergedFields, review.doc_type]);
  const [draftFields, setDraftFields] = useState<Record<string, string>>({});

  useEffect(() => {
    const next: Record<string, string> = {};
    fieldKeys.forEach((key) => {
      next[key] = valueText(confirmedFields[key] ?? displayFieldMap[key] ?? '');
    });
    setDraftFields(next);
  }, [confirmedFields, displayFieldMap, fieldKeys]);

  const submit = (status: 'partial' | 'confirmed') => {
    const payload: Record<string, unknown> = {};
    fieldKeys.forEach((key) => {
      payload[key] = draftFields[key] ?? '';
    });
    onSave(payload, status);
  };

  const warnings = review.validation?.warnings || [];
  const errors = review.validation?.errors || [];
  const evidenceEntries = getKycDisplayEntries(
    Object.fromEntries(Object.entries(review.evidence || {}).map(([field, item]) => {
      const record = item && typeof item === 'object' ? item as Record<string, unknown> : {};
      return [field, record.evidence_text || record.value || ''];
    })),
    review.doc_type
  ).slice(0, 8);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/45 p-4">
      <div className="max-h-[88vh] w-full max-w-5xl overflow-hidden rounded-xl bg-white shadow-xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4">
          <div>
            <div className="text-base font-semibold text-slate-900">{review.doc_type_name || 'KYC资料字段审核'}</div>
            <div className="mt-1 text-xs text-slate-500">
              状态：{review.confirm_status === 'confirmed' ? '已确认' : review.confirm_status === 'partial' ? '部分确认' : '未确认'}
              {review.confirmed_by ? ` · 最近确认人：${review.confirmed_by}` : ''}
            </div>
          </div>
          <button type="button" onClick={onCancel} className="rounded-md px-3 py-1.5 text-sm text-slate-500 hover:bg-slate-100">关闭</button>
        </div>
        <div className="max-h-[calc(88vh-76px)] overflow-auto p-5">
          <div className="overflow-hidden rounded-xl border border-slate-200">
            <div className="grid grid-cols-[160px_1fr_1fr] bg-slate-50 text-xs font-semibold text-slate-600">
              <div className="border-r border-slate-200 px-3 py-2">字段</div>
              <div className="border-r border-slate-200 px-3 py-2">自动提取值</div>
              <div className="px-3 py-2">人工确认值</div>
            </div>
            {fieldKeys.map((key) => (
              <div key={key} className="grid grid-cols-[160px_1fr_1fr] border-t border-slate-200 text-sm">
                <div className="border-r border-slate-200 px-3 py-2 font-medium text-slate-700">{getKycFieldLabel(key)}</div>
                <div className="break-words border-r border-slate-200 px-3 py-2 text-slate-600">{valueText(displayFieldMap[key]) || '未识别'}</div>
                <div className="px-3 py-2">
                  {canEdit ? (
                    <>
                      <input
                        value={draftFields[key] || ''}
                        onChange={(event) => setDraftFields((current) => ({ ...current, [key]: event.target.value }))}
                        className="w-full rounded-md border border-slate-200 px-2 py-1.5 text-sm outline-none focus:border-blue-300"
                      />
                      {key === 'registration_authority' && !valueText(displayFieldMap[key]) ? (
                        <div className="mt-1 text-xs text-amber-600">OCR 未识别到红章登记机关区域，请根据原件人工补录</div>
                      ) : null}
                    </>
                  ) : (
                    <span className="text-slate-800">{draftFields[key] || '未确认'}</span>
                  )}
                </div>
              </div>
            ))}
          </div>

          {(review.missing_fields || []).length > 0 ? (
            <div className="mt-4 rounded-lg bg-amber-50 px-3 py-2 text-sm text-amber-700">
              缺失字段：{(review.missing_fields || []).map(getKycFieldLabel).join('、')}
            </div>
          ) : null}

          {(warnings.length > 0 || errors.length > 0) ? (
            <div className="mt-4 space-y-2">
              {errors.map((item) => <div key={item} className="rounded-lg bg-red-50 px-3 py-2 text-sm text-red-700">{item}</div>)}
              {warnings.map((item) => <div key={item} className="rounded-lg bg-amber-50 px-3 py-2 text-sm text-amber-700">{item}</div>)}
            </div>
          ) : null}

          {evidenceEntries.length > 0 ? (
            <div className="mt-4">
              <div className="mb-2 text-sm font-semibold text-slate-800">证据摘要</div>
              <div className="grid gap-2 md:grid-cols-2">
                {evidenceEntries.map(([field, item]) => {
                  return (
                    <div key={field} className="rounded-lg bg-slate-50 px-3 py-2 text-sm text-slate-600">
                      <span className="font-medium text-slate-700">{getKycFieldLabel(field)}：</span>
                      {valueText(item)}
                    </div>
                  );
                })}
              </div>
            </div>
          ) : null}

          <div className="mt-5 flex justify-end gap-2">
            <button type="button" onClick={onCancel} className="rounded-lg border border-slate-200 px-4 py-2 text-sm text-slate-600 hover:bg-slate-50">取消</button>
            {canEdit ? (
              <>
                <button type="button" disabled={saving} onClick={() => submit('partial')} className="rounded-lg border border-blue-200 bg-blue-50 px-4 py-2 text-sm font-medium text-blue-700 disabled:opacity-50">保存为部分确认</button>
                <button type="button" disabled={saving} onClick={() => submit('confirmed')} className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-50">保存为已确认</button>
              </>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
};

export default KycExtractionReview;
