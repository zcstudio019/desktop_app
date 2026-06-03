import React from 'react';
import type { KycExtractionResult as KycExtractionResultType } from '../services/types';

const FIELD_LABELS: Record<string, string> = {
  name: '姓名',
  gender: '性别',
  ethnicity: '民族',
  birth_date: '出生日期',
  address: '地址',
  id_number: '身份证号码',
  issuing_authority: '签发机关',
  valid_from: '有效期起',
  valid_to: '有效期止',
  company_name: '企业名称',
  unified_social_credit_code: '统一社会信用代码',
  legal_representative: '法定代表人',
  registered_capital: '注册资本',
  company_type: '企业类型',
  establishment_date: '成立日期',
  business_term: '营业期限',
  registered_address: '注册地址',
  business_scope: '经营范围',
  registration_authority: '登记机关',
  issue_date: '发证日期',
  bank_account_name: '账户名称',
  bank_account_number: '银行账号',
  account_name: '户名',
  account_number: '账号',
  opening_bank: '开户银行',
  account_type: '账户类型',
  approval_number: '核准号',
  account_status: '账户状态',
  owner: '权利人',
  co_owners: '共有人',
  certificate_number: '权证编号',
  property_unit_number: '不动产单元号',
  property_address: '房地坐落',
  right_type: '权利类型',
  right_nature: '权属性质',
  acquisition_method: '使用权取得方式',
  land_use: '土地用途',
  use_type: '房屋用途',
  parcel_number: '宗地号',
  building_area: '建筑面积',
  land_area: '宗地面积',
  usage_area: '使用权面积',
  total_area: '使用权面积',
  land_use_term: '土地使用期限',
  room_number: '室号或部位',
  building_type: '建筑类型',
  total_floors: '总层数',
  completion_date: '竣工日期',
  issuing_unit: '填证单位',
  mortgage_status: '抵押状态',
  seizure_status: '查封状态',
  权利人: '权利人',
  共有人: '共有人',
  权证编号: '权证编号',
  房地坐落: '房地坐落',
  权属性质: '权属性质',
  使用权取得方式: '使用权取得方式',
  土地用途: '土地用途',
  宗地号: '宗地号',
  宗地面积: '宗地面积',
  使用权面积: '使用权面积',
  土地使用期限: '土地使用期限',
  室号或部位: '室号或部位',
  建筑面积: '建筑面积',
  建筑类型: '建筑类型',
  房屋用途: '房屋用途',
  总层数: '总层数',
  竣工日期: '竣工日期',
  登记日: '登记日',
  填证单位: '填证单位',
  plate_number: '车牌号码',
  vehicle_owner: '车辆所有人',
  vehicle_type: '车辆类型',
  use_character: '使用性质',
  brand_model: '品牌型号',
  vehicle_identification_number: '车辆识别代号',
  engine_number: '发动机号码',
  registration_date: '注册日期',
  approved_passengers: '核定载人数',
  total_mass: '总质量',
  curb_weight: '整备质量',
  inspection_valid_until: '检验有效期至',
  holder_name: '持证人',
  spouse_name: '配偶姓名',
  holder_id_number: '持证人身份证号码',
  spouse_id_number: '配偶身份证号码',
};

const PROPERTY_FIELD_ORDER = [
  '权利人',
  '共有人',
  '权证编号',
  '房地坐落',
  '权属性质',
  '使用权取得方式',
  '土地用途',
  '宗地号',
  '宗地面积',
  '使用权面积',
  '土地使用期限',
  '室号或部位',
  '建筑面积',
  '建筑类型',
  '房屋用途',
  '总层数',
  '竣工日期',
  '登记日',
  '填证单位',
];

const ENGLISH_TO_CHINESE_FIELDS: Record<string, string> = {
  owner: '权利人',
  co_owners: '共有人',
  certificate_number: '权证编号',
  property_address: '房地坐落',
  right_type: '权利类型',
  right_nature: '权属性质',
  acquisition_method: '使用权取得方式',
  land_use: '土地用途',
  use_type: '房屋用途',
  parcel_number: '宗地号',
  land_area: '宗地面积',
  usage_area: '使用权面积',
  total_area: '使用权面积',
  land_use_term: '土地使用期限',
  room_number: '室号或部位',
  building_area: '建筑面积',
  building_type: '建筑类型',
  total_floors: '总层数',
  completion_date: '竣工日期',
  registration_date: '登记日',
  issue_date: '登记日',
  issuing_unit: '填证单位',
};

const INVALID_DISPLAY_VALUES = new Set(['', '对', '的合法权益，对', '无', '未识别', 'null', 'none']);
const INVALID_DISPLAY_KEYWORDS = ['合法权益', '房地产权利人', '本证是证明', '根据', '法律'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

export function formatKycDisplayValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '未识别';
  if (Array.isArray(value)) return value.length > 0 ? value.map(formatKycDisplayValue).join('、') : '未识别';
  if (isRecord(value)) {
    if ('amount' in value && 'unit' in value) return `${value.amount ?? ''} ${value.unit ?? ''}`.trim();
    if ('value' in value && 'unit' in value) return `${value.value ?? ''} ${value.unit ?? ''}`.trim();
    return Object.entries(value).map(([key, item]) => `${FIELD_LABELS[key] ?? key}: ${formatKycDisplayValue(item)}`).join('，');
  }
  const text = String(value);
  if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(text)) {
    const [year, month, day] = text.split('-');
    return `${year}年${Number(month)}月${Number(day)}日`;
  }
  return text;
}

function isInvalidDisplayValue(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  if (Array.isArray(value)) return value.length === 0 || value.every(isInvalidDisplayValue);
  if (isRecord(value)) return Object.keys(value).length === 0;
  const text = formatKycDisplayValue(value).trim();
  return INVALID_DISPLAY_VALUES.has(text) || INVALID_DISPLAY_KEYWORDS.some((keyword) => text.includes(keyword));
}

export function isKycExtractionResult(value: unknown): value is KycExtractionResultType {
  if (!isRecord(value)) return false;
  return value.agent_type === 'kyc_document_agent' || (typeof value.doc_type === 'string' && isRecord(value.fields));
}

export function getKycFieldLabel(field: string): string {
  return FIELD_LABELS[field] ?? ENGLISH_TO_CHINESE_FIELDS[field] ?? field;
}

export function getKycDisplayFields(fields: Record<string, unknown> | undefined | null, _docType?: string): Array<[string, unknown]> {
  if (!fields || typeof fields !== 'object') return [];
  const display = new Map<string, unknown>();
  const orderedKeys = [...PROPERTY_FIELD_ORDER, ...Object.keys(ENGLISH_TO_CHINESE_FIELDS)];
  orderedKeys.forEach((key) => {
    if (!(key in fields)) return;
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (display.has(label)) return;
    const value = fields[key];
    if (isInvalidDisplayValue(value)) return;
    display.set(label, value);
  });
  Object.entries(fields).forEach(([key, value]) => {
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (display.has(label) || key in ENGLISH_TO_CHINESE_FIELDS) return;
    if (isInvalidDisplayValue(value)) return;
    display.set(label, value);
  });
  return Array.from(display.entries());
}

export function renderKycDisplayMarkdown(result: KycExtractionResultType, fields: Array<[string, unknown]>): string {
  const lines = [
    `## ${result.doc_type_name || 'KYC资料'}`,
    '',
    `- 资料类型编码: ${result.doc_type || 'unknown'}`,
    `- 资料名称: ${result.doc_type_name || 'KYC资料'}`,
    `- 归属类型: ${result.owner_type === 'asset' ? '资产资料' : result.owner_type || '未知'}`,
    `- 提取状态: ${result.extraction_status === 'success' ? '成功' : result.extraction_status === 'partial' ? '部分成功' : '失败'}`,
    '- 处理 Agent: KYC资料识别',
    '',
    '### 关键字段',
  ];
  if (fields.length) {
    fields.forEach(([field, value]) => {
      lines.push(`- ${getKycFieldLabel(field)}: ${formatKycDisplayValue(value)}`);
    });
  } else {
    lines.push('- 无');
  }
  return lines.join('\n');
}

interface Props {
  result: KycExtractionResultType;
}

const KycExtractionResult: React.FC<Props> = ({ result }) => {
  const fields = getKycDisplayFields(result.fields || {}, result.doc_type);
  const displayMarkdown = renderKycDisplayMarkdown(result, fields);
  const warnings = result.validation?.warnings || [];
  const errors = result.validation?.errors || [];
  const displayFieldNames = new Set(fields.map(([field]) => field));
  const evidence = Object.entries(result.evidence || {})
    .map(([field, item]) => [ENGLISH_TO_CHINESE_FIELDS[field] ?? field, item] as const)
    .filter(([field]) => displayFieldNames.has(field))
    .filter(([field], index, entries) => entries.findIndex(([name]) => name === field) === index)
    .slice(0, 6);
  const statusLabel = result.extraction_status === 'success' ? '提取成功' : result.extraction_status === 'partial' ? '部分提取' : '提取失败';

  return (
    <div className="space-y-4 text-sm text-slate-700">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-base font-semibold text-slate-900">{result.doc_type_name || 'KYC资料'}</div>
          <div className="mt-1 text-xs text-slate-500">资料类型编码：{result.doc_type || 'unknown'}</div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">{statusLabel}</span>
          <span className="rounded-full bg-blue-50 px-3 py-1 text-xs font-medium text-blue-700">
            置信度 {typeof result.confidence?.overall === 'number' ? `${Math.round(result.confidence.overall * 100)}%` : '未计算'}
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
                <span className="text-slate-500">{getKycFieldLabel(field)}：</span>{formatKycDisplayValue(value)}
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
                  <span className="font-medium text-slate-700">{getKycFieldLabel(field)}：</span>
                  <span className="text-slate-600">{formatKycDisplayValue(record.evidence_text || record.value || '')}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {displayMarkdown ? (
        <div>
          <div className="mb-2 font-medium text-slate-900">Markdown预览</div>
          <pre className="max-h-56 overflow-auto whitespace-pre-wrap rounded-md bg-slate-900 p-3 text-xs leading-5 text-slate-100">
            {displayMarkdown}
          </pre>
        </div>
      ) : null}
    </div>
  );
};

export default KycExtractionResult;
