export const KYC_DOC_TYPES = new Set([
  'id_card',
  'marriage_cert',
  'marriage_certificate',
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
  company_name: '名称',
  unified_social_credit_code: '统一社会信用代码',
  license_number: '证照编号',
  legal_representative: '法定代表人',
  registered_capital: '注册资本',
  company_type: '类型',
  establishment_date: '成立日期',
  business_term: '营业期限',
  registered_address: '住所',
  business_scope: '经营范围',
  registration_authority: '登记机关',
  issue_date: '发照日期',
  bank_account_name: '账户名称',
  bank_account_number: '账号',
  account_name: '户名',
  account_number: '账号',
  opening_bank: '开户银行',
  account_type: '账户类型',
  approval_number: '核准号',
  basic_account_number: '基本存款账户编号',
  account_status: '账户状态',
  owner: '权利人',
  co_owners: '共有人',
  co_ownership: '共有情况',
  shared_status: '共有情况',
  ownership_status: '共有情况',
  certificate_number: '权证编号',
  property_unit_number: '不动产单元号',
  real_estate_unit_no: '不动产单元号',
  real_estate_unit_number: '不动产单元号',
  property_address: '房地坐落',
  location: '坐落',
  right_type: '权利类型',
  right_nature: '权利性质',
  acquisition_method: '使用权取得方式',
  land_use: '土地用途',
  use_type: '房屋用途',
  house_use: '房屋用途',
  building_use: '房屋用途',
  parcel_number: '宗地号',
  land_area: '宗地面积',
  usage_area: '使用权面积',
  total_area: '使用权面积',
  land_use_term: '使用期限',
  use_term: '使用期限',
  room_number: '室号或部位',
  building_area: '建筑面积',
  building_type: '建筑类型',
  total_floors: '总层数',
  completion_date: '竣工日期',
  registration_date: '登记日',
  issuing_unit: '填证单位',
  mortgage_status: '抵押状态',
  seizure_status: '查封状态',
  plate_number: '号牌号码',
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
  household_info: '户信息',
  household_records: '户信息记录',
  members: '家庭成员',
  household_type: '户别',
  household_number: '户号',
  household_head: '户主姓名',
  household_address: '住址',
  booklet_number: '户口簿编号',
  undertaker: '承办人签章',
  address_change_records: '住址变动记录',
  former_name: '曾用名',
  relationship_to_head: '与户主关系',
  birth_place: '出生地',
  native_place: '籍贯',
  other_address: '本市县其他住址',
  education_level: '文化程度',
  military_status: '兵役状况',
  height: '身高',
  blood_type: '血型',
  religion: '宗教信仰',
  service_place: '服务处所',
  occupation: '职业',
  migration_to_city: '何时由何地迁来本市（县）',
  migration_to_address: '何时由何地迁来本址',
  page_index: '来源页码',
  holder_name: '持证人',
  spouse_name: '配偶姓名',
  holder_id_number: '持证人身份证号码',
  spouse_id_number: '配偶身份证号码',
  certificate_no: '结婚证字号',
  marital_status: '婚姻状态',
  marriage_date: '登记日期',
  holder_1_name: '配偶一姓名',
  holder_1_id_number: '配偶一身份证号',
  holder_raw_id_number: '配偶一疑似身份证号',
  holder_suspected_id_number: '配偶一疑似身份证号',
  holder_2_name: '配偶二姓名',
  holder_2_id_number: '配偶二身份证号',
  spouse_raw_id_number: '配偶二疑似身份证号',
  spouse_suspected_id_number: '配偶二疑似身份证号',
  权利人: '权利人',
  共有人: '共有人',
  共有情况: '共有情况',
  权证编号: '权证编号',
  坐落: '坐落',
  房地坐落: '房地坐落',
  不动产单元号: '不动产单元号',
  权利类型: '权利类型',
  权属性质: '权属性质',
  权利性质: '权利性质',
  使用权取得方式: '使用权取得方式',
  土地用途: '土地用途',
  用途: '土地用途',
  宗地号: '宗地号',
  地号: '地号',
  宗地面积: '宗地面积',
  使用权面积: '使用权面积',
  使用期限: '使用期限',
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
  '共有情况',
  '权证编号',
  '房地坐落',
  '封面编号',
  '坐落',
  '不动产单元号',
  '权利类型',
  '权利性质',
  '权属性质',
  '使用权取得方式',
  '土地用途',
  '房屋用途',
  '地号',
  '宗地号',
  '宗地面积',
  '使用期限',
  '土地使用期限',
  '室号或部位',
  '建筑面积',
  '建筑类型',
  '总层数',
  '竣工日期',
  '登记日期',
  '登记机构',
  '登记日',
  '填证单位',
];

export const BUSINESS_LICENSE_FIELD_ORDER = [
  'unified_social_credit_code',
  'license_number',
  'company_name',
  'company_type',
  'legal_representative',
  'registered_capital',
  'establishment_date',
  'business_term',
  'registered_address',
  'business_scope',
  'registration_authority',
  'issue_date',
];

export const VEHICLE_LICENSE_FIELD_ORDER = [
  'plate_number',
  'vehicle_type',
  'owner',
  'address',
  'use_character',
  'brand_model',
  'vin',
  'engine_number',
  'registration_date',
  'issue_date',
  'approved_passengers',
  'total_mass',
  'curb_weight',
  'inspection_valid_until',
];

export const ACCOUNT_FIELD_ORDER = [
  'company_name',
  'bank_account_name',
  'bank_account_number',
  'opening_bank',
  'account_type',
  'approval_number',
  'basic_account_number',
  'account_status',
  'legal_representative',
  'issue_date',
];

export const HOUSEHOLD_INFO_FIELD_ORDER = [
  'household_type',
  'household_number',
  'household_head',
  'household_address',
  'issuing_authority',
  'issue_date',
  'booklet_number',
  'undertaker',
];

export const HOUSEHOLD_MEMBER_FIELD_ORDER = [
  'name',
  'relationship_to_head',
  'gender',
  'ethnicity',
  'birth_place',
  'native_place',
  'birth_date',
  'id_number',
  'education_level',
  'marital_status',
  'service_place',
  'occupation',
];

const ACCOUNT_FIELD_LABELS: Record<string, string> = {
  company_name: '单位名称',
  bank_account_name: '账户名称',
  bank_account_number: '账号',
  opening_bank: '开户银行',
  account_type: '账户类型',
  approval_number: '核准号',
  basic_account_number: '基本存款账户编号',
  legal_representative: '法定代表人/单位负责人',
  issue_date: '发证日期',
  account_status: '账户状态',
};

const HOUSEHOLD_INFO_FIELD_LABELS: Record<string, string> = {
  household_type: '户别',
  household_number: '户号',
  household_head: '户主姓名',
  household_address: '住址',
  booklet_number: '户口簿编号',
  issuing_authority: '签发机关',
  issue_date: '签发日期',
  undertaker: '承办人签章',
  address_change_records: '住址变动记录',
};

const HOUSEHOLD_MEMBER_FIELD_LABELS: Record<string, string> = {
  name: '姓名',
  former_name: '曾用名',
  relationship_to_head: '与户主关系',
  gender: '性别',
  ethnicity: '民族',
  birth_place: '出生地',
  native_place: '籍贯',
  birth_date: '出生日期',
  other_address: '本市县其他住址',
  id_number: '公民身份号码',
  education_level: '文化程度',
  marital_status: '婚姻状况',
  military_status: '兵役状况',
  height: '身高',
  blood_type: '血型',
  religion: '宗教信仰',
  service_place: '服务处所',
  occupation: '职业',
  migration_to_city: '何时由何地迁来本市（县）',
  migration_to_address: '何时由何地迁来本址',
  registration_date: '登记日期',
  page_index: '来源页码',
};

const VEHICLE_LICENSE_FIELD_LABELS: Record<string, string> = {
  plate_number: '号牌号码',
  vehicle_type: '车辆类型',
  owner: '所有人',
  address: '住址',
  use_character: '使用性质',
  brand_model: '品牌型号',
  vin: '车辆识别代号',
  engine_number: '发动机号码',
  registration_date: '注册日期',
  issue_date: '发证日期',
  approved_passengers: '核定载人数',
  total_mass: '总质量',
  curb_weight: '整备质量',
  inspection_valid_until: '检验有效期止',
  household_info: '户信息',
  household_records: '户信息记录',
  members: '家庭成员',
  household_type: '户别',
  household_number: '户号',
  household_head: '户主姓名',
  household_address: '住址',
  booklet_number: '户口簿编号',
  undertaker: '承办人签章',
  address_change_records: '住址变动记录',
  former_name: '曾用名',
  relationship_to_head: '与户主关系',
  birth_place: '出生地',
  native_place: '籍贯',
  other_address: '本市县其他住址',
  education_level: '文化程度',
  military_status: '兵役状况',
  height: '身高',
  blood_type: '血型',
  religion: '宗教信仰',
  service_place: '服务处所',
  occupation: '职业',
  migration_to_city: '何时由何地迁来本市（县）',
  migration_to_address: '何时由何地迁来本址',
  page_index: '来源页码',
};

const ENGLISH_TO_CHINESE_FIELDS: Record<string, string> = {
  owner: '权利人',
  co_owners: '共有人',
  co_ownership: '共有情况',
  shared_status: '共有情况',
  ownership_status: '共有情况',
  certificate_number: '权证编号',
  cover_certificate_number: '封面编号',
  property_address: '房地坐落',
  property_unit_number: '不动产单元号',
  real_estate_unit_no: '不动产单元号',
  real_estate_unit_number: '不动产单元号',
  right_type: '权利类型',
  right_nature: '权利性质',
  acquisition_method: '使用权取得方式',
  land_use: '土地用途',
  use_type: '房屋用途',
  house_use: '房屋用途',
  building_use: '房屋用途',
  parcel_number: '宗地号',
  land_area: '宗地面积',
  usage_area: '使用权面积',
  total_area: '使用权面积',
  land_use_term: '使用期限',
  use_term: '使用期限',
  location: '坐落',
  address: '坐落',
  room_number: '室号或部位',
  building_area: '建筑面积',
  building_type: '建筑类型',
  total_floors: '总层数',
  completion_date: '竣工日期',
  registration_date: '登记日期',
  registration_authority: '登记机构',
  issue_date: '登记日',
  issuing_unit: '填证单位',
  bank_account_name: '账户名称',
  bank_account_number: '账号',
  opening_bank: '开户银行',
  account_type: '账户类型',
  approval_number: '核准号',
  basic_account_number: '基本存款账户编号',
  account_status: '账户状态',
  plate_number: '号牌号码',
  vehicle_type: '车辆类型',
  vehicle_owner: '所有人',
  use_character: '使用性质',
  brand_model: '品牌型号',
  vin: '车辆识别代号',
  vehicle_identification_number: '车辆识别代号',
  engine_number: '发动机号码',
  approved_passengers: '核定载人数',
  total_mass: '总质量',
  curb_weight: '整备质量',
  inspection_valid_until: '检验有效期止',
};

const INVALID_DISPLAY_VALUES = new Set(['', '对', '的合法权益，对', '无', '未识别', 'null', 'none']);
const INVALID_DISPLAY_KEYWORDS = ['合法权益', '房地产权利人', '本证是证是', '根据', '法律'];
const FORBIDDEN_KYC_DISPLAY_KEYS = new Set([
  'historical_financial_reports',
  'financial_reports',
  'enterprise_credit_reports',
  'personal_credit_reports',
  'enterprise_flows',
  'bank_flows',
  'financial_statement_diagnostic',
  'financing_diagnostic_report',
  'comprehensive_financing_advice',
  'customer_profile_markdown',
  'customer_context',
  'customer_profile',
  'profile_context',
]);

const RAW_KYC_MARKDOWN_MARKERS = [
  'doc type',
  'doc_type',
  'doc type name',
  'owner type',
  'fields',
  'validation',
  'confidence',
  'evidence',
  'missing fields',
  'raw text preview',
  'raw_text_preview',
  'metadata',
  'agent type',
  'classification reason',
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function parseMaybeRecord(value: unknown): Record<string, unknown> {
  if (isRecord(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return isRecord(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }
  return {};
}

function looksLikeRawKycMarkdown(value: unknown): boolean {
  const text = typeof value === 'string' ? value.toLowerCase() : '';
  return Boolean(text && RAW_KYC_MARKDOWN_MARKERS.some((marker) => text.includes(marker)));
}

function firstBusinessMarkdown(...values: unknown[]): string {
  const fallback: string[] = [];
  for (const value of values) {
    if (typeof value !== 'string' || !value.trim()) continue;
    const markdown = value.trim();
    if (!looksLikeRawKycMarkdown(markdown)) {
      return markdown;
    }
    fallback.push(markdown);
  }
  return fallback[0] || '';
}

function pickFirstRecord(...values: unknown[]): Record<string, unknown> {
  for (const value of values) {
    const record = parseMaybeRecord(value);
    if (Object.keys(record).length) return record;
  }
  return {};
}

function getFieldsRecord(value: unknown): Record<string, unknown> {
  const record = parseMaybeRecord(value);
  const fields = parseMaybeRecord(record.fields);
  return Object.keys(fields).length ? fields : {};
}

function getConfirmedFieldsRecord(value: unknown): Record<string, unknown> {
  const record = parseMaybeRecord(value);
  const confirmed = parseMaybeRecord(record.confirmed_data ?? record.confirmedData);
  const fields = parseMaybeRecord(confirmed.confirmed_fields ?? confirmed.confirmedFields);
  return Object.keys(fields).length ? fields : {};
}

export function getKycFieldValue(source: unknown, fieldName: string): unknown {
  const root = parseMaybeRecord(source);
  const document = parseMaybeRecord(root.document);
  const extraction = parseMaybeRecord(root.extraction);
  const extractionResult = parseMaybeRecord(root.extraction_result ?? root.extractionResult);
  const latestExtraction = parseMaybeRecord(root.latest_extraction ?? root.latestExtraction);
  const candidatePayloads = [
    root,
    root.content,
    root.extraction_result,
    root.extractionResult,
    root.latest_extraction,
    root.latestExtraction,
    root.extracted_data,
    root.extractedData,
    root.extracted_json,
    root.extractedJson,
    root.data,
    root.agent_result,
    root.agentResult,
    document.extracted_data,
    document.extractedData,
    extraction.extracted_data,
    extraction.extractedData,
    extractionResult.extracted_data,
    extractionResult.extractedData,
    latestExtraction.extracted_data,
    latestExtraction.extractedData,
  ];
  const candidates: Record<string, unknown>[] = [
    ...candidatePayloads.map(getConfirmedFieldsRecord),
    ...candidatePayloads.map(getFieldsRecord),
    parseMaybeRecord(root.fields),
  ];
  for (const fields of candidates) {
    if (fields && Object.prototype.hasOwnProperty.call(fields, fieldName)) {
      return fields[fieldName];
    }
  }
  return undefined;
}

export function normalizeKycExtractionResult(source: unknown): Record<string, unknown> {
  const root = parseMaybeRecord(source);
  const document = parseMaybeRecord(root.document);
  const extraction = parseMaybeRecord(root.extraction);
  const extractionResult = parseMaybeRecord(root.extraction_result ?? root.extractionResult);
  const latestExtraction = parseMaybeRecord(root.latest_extraction ?? root.latestExtraction);
  const payload = pickFirstRecord(
    root.content,
    root.extraction_result,
    root.extractionResult,
    root.latest_extraction,
    root.latestExtraction,
    root.extracted_data,
    root.extractedData,
    root.extracted_json,
    root.extractedJson,
    root.data,
    root.agent_result,
    root.agentResult,
    document.extracted_data,
    document.extractedData,
    extraction.extracted_data,
    extraction.extractedData,
    extractionResult.extracted_data,
    extractionResult.extractedData,
    latestExtraction.extracted_data,
    latestExtraction.extractedData,
    root,
  );
  const fieldNames = Array.from(new Set([
    ...Object.keys(getFieldsRecord(payload)),
    ...Object.keys(getFieldsRecord(root)),
    ...Object.keys(getConfirmedFieldsRecord(root)),
    ...Object.keys(getConfirmedFieldsRecord(payload)),
  ]));
  const fields: Record<string, unknown> = {};
  fieldNames.forEach((fieldName) => {
    const value = getKycFieldValue(source, fieldName);
    if (value !== undefined && value !== null && value !== '') {
      fields[fieldName] = value;
    }
  });
  return {
    ...payload,
    ...root,
    agent_type: root.agent_type || payload.agent_type || 'kyc_document_agent',
    doc_type: root.doc_type || payload.doc_type || root.docType || payload.docType || root.document_type || payload.document_type || root.documentType || payload.documentType || root.document_type_code || payload.document_type_code || root.documentTypeCode || payload.documentTypeCode || root.extraction_type || payload.extraction_type || '',
    doc_type_name: root.doc_type_name || payload.doc_type_name || root.docTypeName || payload.docTypeName || root.document_type_name || payload.document_type_name || '',
    owner_type: root.owner_type || payload.owner_type || '',
    extraction_status: root.extraction_status || payload.extraction_status || 'partial',
    fields,
    validation: parseMaybeRecord(root.validation).is_valid !== undefined ? root.validation : payload.validation,
    confidence: root.confidence || payload.confidence,
    evidence: root.evidence || payload.evidence || {},
    missing_fields: root.missing_fields || payload.missing_fields || [],
    markdown: firstBusinessMarkdown(
      payload.markdown,
      payload.markdown_summary,
      payload.display_markdown,
      payload.summary_markdown,
      root.markdown,
      root.markdown_content,
      root.markdownContent,
      payload.markdown_content,
      payload.markdownContent,
    ),
  };
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

function isNewRealEstateCert(fields: Record<string, unknown>, display: Map<string, string>): boolean {
  const certNumber = formatKycDisplayValue(fields['权证编号'] ?? fields.certificate_number ?? display.get('权证编号'));
  if (certNumber.includes('房地') || certNumber.includes('房权')) return false;
  return Boolean(
    fields['不动产单元号'] ||
      fields.real_estate_unit_no ||
      fields.real_estate_unit_number ||
      fields['共有情况'] ||
      fields.co_ownership ||
      fields.shared_status ||
      fields.ownership_status ||
      fields['权利类型'] ||
      fields.right_type ||
      fields['权利性质'] ||
      certNumber.includes('不动产权'),
  );
}

function moreCompleteDisplayValue(current?: string, candidate?: string): string {
  const currentText = String(current ?? '').trim();
  const candidateText = String(candidate ?? '').trim();
  if (!currentText) return candidateText;
  if (!candidateText) return currentText;
  if (candidateText === currentText) return currentText;
  const currentScore = currentText.length + (currentText.includes('止') ? 20 : 0) + ((currentText.match(/年/g) || []).length * 4);
  const candidateScore = candidateText.length + (candidateText.includes('止') ? 20 : 0) + ((candidateText.match(/年/g) || []).length * 4);
  return candidateScore > currentScore ? candidateText : currentText;
}

function collapsePropertySynonymFields(display: Map<string, string>, fields: Record<string, unknown>): Map<string, string> {
  const next = new Map(display);
  const isNewVersion = isNewRealEstateCert(fields, next);
  const groups: Array<[string, string[], string[]]> = [
    isNewVersion ? ['坐落', ['房地坐落'], ['property_address', 'address', 'location']] : ['房地坐落', ['坐落'], ['property_address', 'address', 'location']],
    isNewVersion ? ['权利性质', ['权属性质'], ['right_nature']] : ['权属性质', ['权利性质'], ['right_nature']],
    isNewVersion ? ['地号', ['宗地号'], ['parcel_number']] : ['宗地号', ['地号'], ['parcel_number']],
    isNewVersion ? ['使用期限', ['土地使用期限'], ['use_term', 'land_use_term']] : ['土地使用期限', ['使用期限'], ['use_term', 'land_use_term']],
    ['登记日期', ['登记日'], ['registration_date', 'issue_date']],
  ];
  groups.forEach(([preferred, aliases, rawAliases]) => {
    const values = [
      next.get(preferred),
      ...aliases.map((alias) => next.get(alias)),
      ...rawAliases.map((alias) => formatKycDisplayValue(fields[alias])),
    ].filter(Boolean) as string[];
    aliases.forEach((alias) => next.delete(alias));
    if (!values.length) return;
    const best = values.reduce((current, candidate) => moreCompleteDisplayValue(current, candidate), '');
    next.set(preferred, best);
  });
  return next;
}

export function isKycDocType(docType?: unknown): boolean {
  return typeof docType === 'string' && KYC_DOC_TYPES.has(docType);
}

export function getKycDisplayFields(fields: Record<string, unknown> | undefined | null, docType?: string): Record<string, string> {
  if (!fields || typeof fields !== 'object') return {};

  const display = new Map<string, string>();
  const isPropertyCert = docType === 'property_cert' || docType === 'real_estate_cert';
  const isBusinessLicense = docType === 'business_license';
  const isVehicleLicense = docType === 'vehicle_license';
  const isAccountDoc = docType === 'account_permit' || docType === 'basic_account_info';
  const isMarriageCert = docType === 'marriage_certificate' || docType === 'marriage_cert';
  const isHouseholdRegister = docType === 'household_register';
  if (isMarriageCert) {
    const holder1 = parseMaybeRecord(fields.holder_1);
    const holder2 = parseMaybeRecord(fields.holder_2);
    const entries: Array<[string, unknown]> = [
      ['婚姻状态', fields.marital_status || '已婚'],
      ['结婚证字号', fields.certificate_no || fields.certificate_number],
      ['登记机关', fields.registration_authority || fields.issuing_authority],
      ['发证日期', fields.issue_date],
      ['登记日期', fields.marriage_date || fields.registration_date],
      ['配偶一姓名', holder1.name || fields.holder_name || fields.holder_1_name],
      ['配偶一性别', holder1.gender],
      ['配偶一国籍', holder1.nationality],
      ['配偶一出生日期', holder1.birth_date],
      ['配偶一身份证号', holder1.id_number || fields.holder_id_number || fields.holder_1_id_number || '未识别'],
      ['配偶一疑似身份证号', holder1.raw_id_number || holder1.suspected_id_number || fields.holder_raw_id_number || fields.holder_suspected_id_number],
      ['配偶二姓名', holder2.name || fields.spouse_name || fields.holder_2_name],
      ['配偶二性别', holder2.gender],
      ['配偶二国籍', holder2.nationality],
      ['配偶二出生日期', holder2.birth_date],
      ['配偶二身份证号', holder2.id_number || fields.spouse_id_number || fields.holder_2_id_number || '未识别'],
      ['配偶二疑似身份证号', holder2.raw_id_number || holder2.suspected_id_number || fields.spouse_raw_id_number || fields.spouse_suspected_id_number],
    ];
    entries.forEach(([label, value]) => {
      if (isInvalidDisplayValue(value) && !(String(label).includes('身份证号') && value === '未识别')) return;
      const text = formatKycDisplayValue(value);
      if (text) display.set(label, text);
    });
    return Object.fromEntries(display.entries());
  }
  if (isBusinessLicense) {
    BUSINESS_LICENSE_FIELD_ORDER.forEach((key) => {
      const value = fields[key];
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(key, text);
    });
    Object.entries(fields).forEach(([key, value]) => {
      if (display.has(key) || FORBIDDEN_KYC_DISPLAY_KEYS.has(key)) return;
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(key, text);
    });
    return Object.fromEntries(display.entries());
  }
  if (isVehicleLicense) {
    VEHICLE_LICENSE_FIELD_ORDER.forEach((key) => {
      const value = fields[key];
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(VEHICLE_LICENSE_FIELD_LABELS[key] ?? key, text);
    });
    Object.entries(fields).forEach(([key, value]) => {
      const label = VEHICLE_LICENSE_FIELD_LABELS[key] ?? ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
      if (display.has(label) || FORBIDDEN_KYC_DISPLAY_KEYS.has(key)) return;
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(label, text);
    });
    return Object.fromEntries(display.entries());
  }
  if (isAccountDoc) {
    ACCOUNT_FIELD_ORDER.forEach((key) => {
      const value = fields[key];
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(ACCOUNT_FIELD_LABELS[key] ?? key, text);
    });
    Object.entries(fields).forEach(([key, value]) => {
      const label = ACCOUNT_FIELD_LABELS[key] ?? ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
      if (display.has(label) || FORBIDDEN_KYC_DISPLAY_KEYS.has(key)) return;
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(label, text);
    });
    return Object.fromEntries(display.entries());
  }
  if (isHouseholdRegister) {
    const householdInfo = parseMaybeRecord(fields.household_info);
    HOUSEHOLD_INFO_FIELD_ORDER.forEach((key) => {
      const value = householdInfo[key] ?? fields[key];
      if (isInvalidDisplayValue(value)) return;
      const text = formatKycDisplayValue(value);
      if (!text) return;
      display.set(HOUSEHOLD_INFO_FIELD_LABELS[key] ?? key, text);
    });
    const members = Array.isArray(fields.members) ? fields.members.filter(isRecord) : [];
    const householdRecords = Array.isArray(fields.household_records) ? fields.household_records.filter(isRecord) : [];
    householdRecords.forEach((record, index) => {
      const parts = HOUSEHOLD_INFO_FIELD_ORDER
        .map((key) => {
          const value = record[key];
          if (isInvalidDisplayValue(value)) return '';
          const text = formatKycDisplayValue(value);
          return text ? `${HOUSEHOLD_INFO_FIELD_LABELS[key] ?? key}：${text}` : '';
        })
        .filter(Boolean);
      if (parts.length > 0) display.set(`户信息 ${index + 1}`, parts.join('；'));
    });
    members.forEach((member, index) => {
      const name = formatKycDisplayValue(member.name) || `成员${index + 1}`;
      const parts = HOUSEHOLD_MEMBER_FIELD_ORDER
        .filter((key) => key !== 'name')
        .map((key) => {
          const value = member[key];
          if (isInvalidDisplayValue(value)) return '';
          const text = formatKycDisplayValue(value);
          return text ? `${HOUSEHOLD_MEMBER_FIELD_LABELS[key] ?? key}：${text}` : '';
        })
        .filter(Boolean);
      display.set(`成员 ${index + 1}：${name}`, parts.join('；'));
    });
    return Object.fromEntries(display.entries());
  }
  const ownerDisplay = isPropertyCert ? mergeOwnerFields(fields) : '';
  if (ownerDisplay) {
    display.set('权利人', ownerDisplay);
  }
  const orderedKeys = isPropertyCert
    ? [...PROPERTY_CERT_FIELD_ORDER, ...Object.keys(ENGLISH_TO_CHINESE_FIELDS)]
    : [...Object.keys(FIELD_LABELS), ...Object.keys(ENGLISH_TO_CHINESE_FIELDS), ...Object.keys(fields)];

  orderedKeys.forEach((key) => {
    if (!(key in fields)) return;
    if (FORBIDDEN_KYC_DISPLAY_KEYS.has(key)) return;
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (display.has(label)) return;
    const value = fields[key];
    if (isInvalidDisplayValue(value) || (isPropertyCert && isHiddenPropertyField(label, value))) return;
    const text = formatKycDisplayValue(value);
    if (!text) return;
    display.set(label, text);
  });

  Object.entries(fields).forEach(([key, value]) => {
    if (FORBIDDEN_KYC_DISPLAY_KEYS.has(key)) return;
    const label = ENGLISH_TO_CHINESE_FIELDS[key] ?? key;
    if (isPropertyCert && !PROPERTY_CERT_FIELD_ORDER.includes(label)) return;
    if (display.has(label) || key in ENGLISH_TO_CHINESE_FIELDS) return;
    if (isInvalidDisplayValue(value) || (isPropertyCert && isHiddenPropertyField(label, value))) return;
    const text = formatKycDisplayValue(value);
    if (!text) return;
    display.set(label, text);
  });

  const collapsedDisplay = isPropertyCert ? collapsePropertySynonymFields(display, fields) : display;
  const orderedDisplay = new Map<string, string>();
  if (isPropertyCert) {
    PROPERTY_CERT_FIELD_ORDER.forEach((label) => {
      const value = collapsedDisplay.get(label);
      if (value) orderedDisplay.set(label, value);
    });
    collapsedDisplay.forEach((value, label) => {
      if (!orderedDisplay.has(label)) orderedDisplay.set(label, value);
    });
  }
  const displayFields = Object.fromEntries((isPropertyCert ? orderedDisplay : collapsedDisplay).entries());
  if (isPropertyCert) {
    console.debug('[KycDisplayFields][DEBUG] docType=%s', docType);
    console.debug('[KycDisplayFields][DEBUG] rawFields=', fields);
    console.debug('[KycDisplayFields][DEBUG] raw 使用期限 =', fields['使用期限']);
    console.debug('[KycDisplayFields][DEBUG] raw 土地使用期限 =', fields['土地使用期限']);
    console.debug('[KycDisplayFields][DEBUG] raw land_use_term =', fields.land_use_term);
    console.debug('[KycDisplayFields][DEBUG] raw use_term =', fields.use_term);
    console.debug('[KycDisplayFields][ADDRESS] raw_房地坐落=', fields['房地坐落']);
    console.debug('[KycDisplayFields][ADDRESS] raw_坐落=', fields['坐落']);
    console.debug('[KycDisplayFields][ADDRESS] display_address=', displayFields['房地坐落'] ?? displayFields['坐落']);
    console.debug('[KycDisplayFields][DEBUG] display 使用期限 =', displayFields['使用期限']);
    console.debug('[KycDisplayFields][DEBUG] displayFields=', displayFields);
  }
  return displayFields;
}

export function getKycDisplayEntries(fields: Record<string, unknown> | undefined | null, docType?: string): Array<[string, string]> {
  return Object.entries(getKycDisplayFields(fields, docType));
}
