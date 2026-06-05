import React from 'react';
import type { CustomerKycProfile } from '../services/types';
import { formatKycDisplayValue, getKycFieldLabel } from '../utils/kycDisplayFields';

function valueText(value: unknown): string {
  if (value === null || value === undefined || value === '') return '未识别';
  if (typeof value === 'object') {
    if (Array.isArray(value)) return value.length ? value.map(valueText).join('、') : '未识别';
    const record = value as Record<string, unknown>;
    if ('amount' in record && 'unit' in record) return formatKycDisplayValue(value);
    if ('value' in record && 'unit' in record) return formatKycDisplayValue(value);
  }
  return String(value);
}

function hasAnyValue(record?: Record<string, unknown>): boolean {
  return Boolean(record && Object.values(record).some((value) => value !== null && value !== undefined && value !== ''));
}

function FieldGrid({ fields, data }: { fields: string[]; data?: Record<string, unknown> }) {
  if (!hasAnyValue(data)) {
    return <div className="rounded-lg bg-slate-50 px-3 py-2 text-sm text-slate-500">暂无资料</div>;
  }
  return (
    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
      {fields.map((field) => (
        <div key={field} className="rounded-lg border border-slate-200 bg-white px-3 py-2">
          <div className="text-xs text-slate-500">{getKycFieldLabel(field)}</div>
          <div className="mt-1 break-words text-sm font-medium text-slate-800">{valueText(data?.[field])}</div>
        </div>
      ))}
    </div>
  );
}

const KycProfilePanel: React.FC<{
  profile: CustomerKycProfile | null;
  loading?: boolean;
  onReviewDocument?: (documentId: string) => void;
}> = ({ profile, loading, onReviewDocument }) => {
  const properties = profile?.assets?.properties || [];
  const vehicles = profile?.assets?.vehicles || [];
  const licenses = profile?.licenses || [];
  const hasKycDocuments = Boolean((profile?.documents || []).length);

  if (loading) {
    return <section className="border-b border-slate-200 bg-white px-6 py-5 text-sm text-slate-500">正在加载 KYC 资料...</section>;
  }

  return (
    <section className="border-b border-slate-200 bg-white px-6 py-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">KYC客户画像</h3>
          <p className="mt-1 text-sm text-slate-500">由已上传证照、账户、资产和资质资料实时聚合生成。</p>
        </div>
        <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-600">
          已识别 {(profile?.documents || []).length} 份 KYC 资料
        </span>
      </div>

      {!hasKycDocuments ? (
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-center text-sm text-slate-500">
          暂无 KYC 资料
        </div>
      ) : (
        <div className="space-y-5">
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">企业基础信息</h4>
            <FieldGrid
              data={profile?.enterprise_identity}
              fields={['company_name', 'unified_social_credit_code', 'legal_representative', 'registered_capital', 'registered_address', 'business_scope']}
            />
          </div>
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">法人/个人身份信息</h4>
            <FieldGrid data={profile?.person_identity} fields={['name', 'id_number', 'gender', 'birth_date', 'address']} />
          </div>
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">银行账户信息</h4>
            <FieldGrid data={profile?.bank_account} fields={['account_name', 'account_number', 'opening_bank', 'account_type']} />
          </div>
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">资产信息</h4>
            <div className="grid gap-3 xl:grid-cols-2">
              <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                <div className="mb-2 text-sm font-medium text-slate-700">房产列表</div>
                {properties.length ? properties.map((item, index) => (
                  <div key={`${item.source_document_id || index}`} className="mb-2 rounded-lg bg-white px-3 py-2 text-sm text-slate-700 last:mb-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span>{valueText(item.property_address)} · 权利人：{valueText(item.owner)}</span>
                      <span className={`rounded-full border px-2 py-0.5 text-xs ${
                        index === 0
                          ? 'border-amber-200 bg-amber-50 text-amber-700'
                          : 'border-slate-200 bg-slate-50 text-slate-500'
                      }`}>
                        {valueText(item.display_role || (index === 0 ? '主资料 / 字段完整' : '补充页'))}
                      </span>
                    </div>
                    {item.source_file ? (
                      <div className="mt-1 text-xs text-slate-500">资料来源：{valueText(item.source_file)}</div>
                    ) : null}
                  </div>
                )) : <div className="text-sm text-slate-500">暂无房产资料</div>}
              </div>
              <div className="rounded-xl border border-slate-200 bg-slate-50 p-3">
                <div className="mb-2 text-sm font-medium text-slate-700">车辆列表</div>
                {vehicles.length ? vehicles.map((item, index) => (
                  <div key={`${item.source_document_id || index}`} className="mb-2 rounded-lg bg-white px-3 py-2 text-sm text-slate-700 last:mb-0">
                    {valueText(item.plate_number)} · 所有人：{valueText(item.vehicle_owner)}
                  </div>
                )) : <div className="text-sm text-slate-500">暂无车辆资料</div>}
              </div>
            </div>
          </div>
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">经营资质</h4>
            {licenses.length ? (
              <div className="grid gap-2 md:grid-cols-2">
                {licenses.map((item, index) => (
                  <div key={`${item.source_document_id || index}`} className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700">
                    <div className="font-medium text-slate-800">{valueText(item.doc_type_name || item.name)}</div>
                    <div className="mt-1 text-xs text-slate-500">证号：{valueText(item.certificate_number)}</div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="rounded-lg bg-slate-50 px-3 py-2 text-sm text-slate-500">暂无经营资质资料</div>
            )}
          </div>
          <div>
            <h4 className="mb-2 text-sm font-semibold text-slate-800">KYC资料审核</h4>
            <div className="grid gap-2 md:grid-cols-2">
              {(profile?.documents || []).map((item, index) => {
                const docId = String(item.doc_id || '');
                return (
                  <div key={`${docId || index}`} className="flex items-center justify-between gap-3 rounded-lg border border-slate-200 bg-white px-3 py-2">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium text-slate-800">{valueText(item.doc_type_name || item.doc_type)}</div>
                      <div className="mt-0.5 truncate text-xs text-slate-500">{valueText(item.source_file || docId)}</div>
                    </div>
                    {docId && onReviewDocument ? (
                      <button
                        type="button"
                        onClick={() => onReviewDocument(docId)}
                        className="shrink-0 rounded-md border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-medium text-blue-700 hover:bg-blue-100"
                      >
                        审核字段
                      </button>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </section>
  );
};

export default KycProfilePanel;
