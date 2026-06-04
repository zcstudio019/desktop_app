export const KYC_DOC_TYPES = new Set([
  'id_card',
  'marriage_cert',
  'divorce_cert',
  'household_register',
  'business_license',
  'account_permit',
  'basic_account_info',
  'vehicle_license',
  'driving_license',
  'property_cert',
  'real_estate_cert',
  'lease_contract_keypage',
  'real_estate_query',
  'shareholder_id_card',
  'articles_keypage',
  'special_business_license',
  'food_business_license',
  'road_transport_license',
  'account_receipt',
  'taxpayer_qualification',
]);

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
  land_use: '用途',
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
  issuing_unit: '填证单位',
  mortgage_status: '抵押状态',
  seizure_status: '查封状态',
  plate_number: '车牌号码',
  vehicle_owner: '车辆所有人',
  vehicle_type: '车辆类型',
  use_character: '使用性质',
  brand_model: '品牌型号',
  vehicle_identification_number: '车辆识别代号',
  engine_number: '发动机号码',
  approved_passengers: '核定载人数',
  total_mass: '总质量',
  curb_weight: '整备质量',
  inspection_valid_until: '检验有效期至',
  holder_name: '持证人',
  spouse_name: '配偶姓名',
  holder_id_number: '持证人身份证号码',
  spouse_id_number: '配偶身份证号码',
  权利人: '权利人',
  共有人: '共有人',
  权证编号: '权证编号',
  房地坐落: '房地坐落',
  权属性质: '权属性质',
  使用权取得方式: '使用权取得方式',
  土地用途: '用途',
  用途: '用途',
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
};

export const PROPERTY_CERT_FIELD_ORDER = [
  '权利人',
  '权证编号',
  '房地坐落',
  '权属性质',
  '使用权取得方式',
  '用途',
  '宗地号',
  '宗地面积',
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
  land_use: '用途',
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
const INVALID_DISPLAY_KEYWORDS = ['合法权益', '房地产权利人', '本证是证是', '根据', '法律'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

export function getKycFieldLabel(field: string): string {
  return FIELD_LABELS[field] ?? ENGLISH_TO_CHINESE_FIELDS[field] ?? field;
}

export function formatKycDisplayValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '';
  if (Array.isArray(value)) return value.map(formatKycDisplayValue).filter(Boolean).join('、');
  if (isRecord(value)) {
    if ('amount' in value && 'unit' in value) return `${value.amount ?? ''} ${value.unit ?? ''}`.trim();
    if ('value' in value && 'unit' in value) return `${value.value ?? ''} ${value.unit ?? ''}`.trim();
    return Object.entries(value)
      .map(([key, item]) => `${getKycFieldLabel(key)}: ${formatKycDisplayValue(item)}`)
      .filter((item) => !item.endsWith(': '))
      .join('，');
  }
  const text = String(value).trim();
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

function mergeOwnerFields(fields: Record<string, unknown>): string {
  const owner = fields['权利人'] ?? fields.owner;
  const coOwners = fields['共有人'] ?? fields.co_owners;
  const values = [owner, coOwners]
    .map(formatKycDisplayValue)
    .flatMap((value) => value.replace(/[,，]/g, '、').split('、'))
    .map((value) => value.trim())
    .filter(Boolean);
  return Array.from(new Set(values)).join('、');
}

function isHiddenPropertyField(label: string, value: unknown): boolean {
  if (label === '共有人') return true;
  if (label === '使用权面积') {
    const text = formatKycDisplayValue(value).trim();
    return !text || text === '独用' || !/\d/.test(text);
  }
  if (label === '竣工日期') {
    const text = formatKycDisplayValue(value).trim();
    return !text.includes('年') && !/^\d{4}-\d{1,2}-\d{1,2}$/.test(text);
  }
  return false;
}

export function isKycDocType(docType?: unknown): boolean {
  return typeof docType === 'string' && KYC_DOC_TYPES.has(docType);
}

export function getKycDisplayFields(fields: Record<string, unknown> | undefined | null, docType?: string): Record<string, string> {
  if (!fields || typeof fields !== 'object') return {};

  const display = new Map<string, string>();
  const isPropertyCert = docType === 'property_cert' || docType === 'real_estate_cert';
  const ownerDisplay = isPropertyCert ? mergeOwnerFields(fields) : '';
  if (ownerDisplay) {
    display.set('权利人', ownerDisplay);
  }
  const orderedKeys = isPropertyCert
    ? [...PROPERTY_CERT_FIELD_ORDER, ...Object.keys(ENGLISH_TO_CHINESE_FIELDS)]
    : [...Object.keys(FIELD_LABELS), ...Object.keys(ENGLISH_TO_CHINESE_FIELDS), ...Object.keys(fields)];

  orderedKeys.forEach((key) => {
    if (!(key in fields)) return;
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (display.has(label)) return;
    const value = fields[key];
    if (isInvalidDisplayValue(value) || (isPropertyCert && isHiddenPropertyField(label, value))) return;
    const text = formatKycDisplayValue(value);
    if (!text) return;
    display.set(label, text);
  });

  Object.entries(fields).forEach(([key, value]) => {
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (display.has(label) || key in ENGLISH_TO_CHINESE_FIELDS) return;
    if (isInvalidDisplayValue(value) || (isPropertyCert && isHiddenPropertyField(label, value))) return;
    const text = formatKycDisplayValue(value);
    if (!text) return;
    display.set(label, text);
  });

  return Object.fromEntries(display.entries());
}

export function getKycDisplayEntries(fields: Record<string, unknown> | undefined | null, docType?: string): Array<[string, string]> {
  return Object.entries(getKycDisplayFields(fields, docType));
}
