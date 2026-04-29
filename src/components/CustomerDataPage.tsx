import React, { useCallback, useEffect, useMemo, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ArrowLeft, ChevronDown, ChevronRight, Download, Eye, FileText, Pencil, RefreshCw, Save, Trash2 } from 'lucide-react';
import {
  deleteCustomer,
  deleteCustomerProfileMarkdown,
  downloadDocumentOriginal,
  getCustomerDocuments,
  getCustomerExtractions,
  getCustomerProfileMarkdown,
  listCustomers,
  previewDocumentOriginal,
  updateCustomerProfileMarkdown,
} from '../services/api';
import type { CustomerDocumentListItem, CustomerListItem, CustomerProfileMarkdownResponse, ExtractionGroup, ExtractionItem } from '../services/types';
import { useApp } from '../context/AppContext';
import ProcessFeedbackCard from './common/ProcessFeedbackCard';

interface CustomerDataPageProps {
  onBack?: () => void;
}

type EditorMode = 'edit' | 'preview';
type DocumentFilterMode = 'all' | 'original' | 'extraction';
type CompletenessStatus = 'complete' | 'pending' | 'empty' | 'available';
type ReadinessStatus = 'ready' | 'suggest' | 'blocked';
type ActionType = 'generate_application' | 'scheme_match' | 'upload_missing';
type FieldConsistencyStatus = 'consistent' | 'conflict' | 'insufficient';

interface CompletenessCard {
  key: string;
  title: string;
  description: string;
  status: CompletenessStatus;
  existingLabels: string[];
  missingRequiredLabels: string[];
  missingOptionalLabels: string[];
}

interface ReadinessItem {
  key: 'application' | 'matching' | 'risk';
  title: string;
  status: ReadinessStatus;
  message: string;
  missingLabels: string[];
  suggestion?: string;
}

interface ReadinessSummary {
  overallStatus: ReadinessStatus;
  overallTitle: string;
  overallDescription: string;
  items: ReadinessItem[];
  actions: ActionType[];
  missingLabels: string[];
}

interface FieldSourceRule {
  fieldKey: string;
  label: string;
  sourceTypes: string[];
  valueLabels: string[];
}

interface FieldSourceSummary {
  fieldKey: string;
  label: string;
  value: string;
  sources: CustomerDocumentListItem[];
}

interface FieldConsistencySourceValue {
  extractionId: string;
  documentType: string;
  documentTypeName: string;
  fileName: string;
  rawValue: string;
  normalizedValue: string;
  document?: CustomerDocumentListItem;
}

interface FieldConsistencyResult {
  fieldKey: string;
  label: string;
  status: FieldConsistencyStatus;
  comparedSources: FieldConsistencySourceValue[];
}

interface CompanyArticlesInsight {
  companyName: string;
  registeredCapital: string;
  businessScope: string;
  address: string;
  managementStructure: string;
  summary: string;
  legalPerson: string;
  executiveDirector: string;
  chairman: string;
  manager: string;
  supervisor: string;
  managementRolesSummary: string;
  managementRoleEvidenceLines: string[];
  shareholderCount: string;
  equityStructureSummary: string;
  shareholders: Array<Record<string, unknown>>;
  financingApprovalRule: string;
  financingApprovalThreshold: string;
  majorDecisionRuleDetails: Array<Record<string, unknown>>;
  document?: CustomerDocumentListItem;
}

interface CompanyArticlesShareholderView {
  name: string;
  contribution: string;
  method: string;
  date: string;
  ratio: string;
  ratioNumber: number | null;
  isPrimary: boolean;
}

interface CompanyArticlesRuleDetailView {
  topic: string;
  rule: string;
  threshold: string;
}

interface CompanyArticlesControlSummary {
  label: string;
  description: string;
}

interface CompanyArticlesRuleGroupView {
  topic: string;
  items: CompanyArticlesRuleDetailView[];
}

interface HukouInsight {
  householdHeadName: string;
  householdNumber: string;
  householdType: string;
  householdAddress: string;
  registrationAuthority: string;
  completenessNote: string;
  members: Array<Record<string, unknown>>;
  document?: CustomerDocumentListItem;
}

const DOCUMENT_GROUPS = {
  enterprise: {
    title: '企业主体资料',
    types: ['business_license', 'company_articles', 'special_license'],
  },
  banking: {
    title: '银行资料',
    types: ['account_license', 'bank_statement', 'bank_statement_detail'],
  },
  personal: {
    title: '个人/家庭资料',
    types: ['id_card', 'hukou', 'marriage_cert'],
  },
  asset: {
    title: '资产资料',
    types: ['property_report', 'vehicle_license'],
  },
} as const;

const DOCUMENT_GROUP_ORDER = ['enterprise', 'banking', 'personal', 'asset'] as const;

const DOCUMENT_COMPLETENESS_RULES = {
  enterprise: {
    title: '企业主体资料',
    description: '用于确认主体资格、基础资质与企业设立信息。',
    required: ['business_license'],
    optional: ['company_articles', 'special_license'],
  },
  banking: {
    title: '银行资料',
    description: '用于核对账户主体、账户状态与银行流水情况。',
    required: ['account_license'],
    optional: ['bank_statement', 'bank_statement_detail'],
  },
  personal: {
    title: '个人/家庭资料',
    description: '用于补充实际控制人、婚姻及家庭关系信息。',
    required: ['id_card'],
    optional: ['hukou', 'marriage_cert'],
  },
  asset: {
    title: '资产资料',
    description: '用于补充房产、车辆等可支持授信判断的资产信息。',
    required: [],
    optional: ['property_report', 'vehicle_license'],
  },
} as const;

const FIELD_SOURCE_RULES: FieldSourceRule[] = [
  {
    fieldKey: 'personal_name',
    label: '姓名',
    sourceTypes: ['id_card', 'hukou', 'marriage_cert'],
    valueLabels: ['name', '姓名', 'household_head_name', '户主姓名', 'husband_name', 'wife_name'],
  },
  {
    fieldKey: 'spouse_name',
    label: '配偶姓名',
    sourceTypes: ['marriage_cert'],
    valueLabels: ['wife_name', 'husband_name', 'persons'],
  },
  {
    fieldKey: 'company_name',
    label: '公司名称',
    sourceTypes: ['business_license', 'company_articles'],
    valueLabels: ['公司名称', '企业名称', '名称', 'company_name', 'account_name'],
  },
  {
    fieldKey: 'credit_code',
    label: '统一社会信用代码',
    sourceTypes: ['business_license'],
    valueLabels: ['统一社会信用代码', '社会信用代码', 'credit_code'],
  },
  {
    fieldKey: 'legal_person',
    label: '法定代表人',
    sourceTypes: ['business_license', 'company_articles'],
    valueLabels: ['法定代表人', '法人', '负责人', 'legal_person'],
  },
  {
    fieldKey: 'registered_capital',
    label: '注册资本',
    sourceTypes: ['business_license', 'company_articles'],
    valueLabels: ['注册资本', 'registered_capital'],
  },
  {
    fieldKey: 'address',
    label: '地址 / 住所',
    sourceTypes: ['business_license', 'company_articles', 'hukou', 'id_card'],
    valueLabels: ['地址', '住所', '营业场所', 'company_address', 'address', 'household_address', '户籍地址'],
  },
  {
    fieldKey: 'bank_name',
    label: '开户行',
    sourceTypes: ['account_license', 'bank_statement'],
    valueLabels: ['开户行', '开户银行', '银行名称', 'bank_name', 'bank_branch'],
  },
  {
    fieldKey: 'account_number',
    label: '账号',
    sourceTypes: ['account_license', 'bank_statement'],
    valueLabels: ['账号', '银行账号', '账户号码', 'account_number'],
  },
  {
    fieldKey: 'id_number',
    label: '身份证号',
    sourceTypes: ['id_card', 'marriage_cert'],
    valueLabels: ['身份证号', '证件号码', 'id_number', 'husband_id_number', 'wife_id_number'],
  },
  {
    fieldKey: 'marriage_registration_date',
    label: '结婚登记日期',
    sourceTypes: ['marriage_cert'],
    valueLabels: ['registration_date', '结婚登记日期', '登记日期'],
  },
  {
    fieldKey: 'marriage_registration_authority',
    label: '结婚登记机关',
    sourceTypes: ['marriage_cert'],
    valueLabels: ['registration_authority', '登记机关', '婚姻登记机关'],
  },
  {
    fieldKey: 'household_member_names',
    label: '家庭成员姓名',
    sourceTypes: ['hukou'],
    valueLabels: ['members', 'household_head_name', '户主姓名'],
  },
  {
    fieldKey: 'household_member_id_numbers',
    label: '家庭成员身份证号',
    sourceTypes: ['hukou'],
    valueLabels: ['members'],
  },
  {
    fieldKey: 'household_relationships',
    label: '家庭成员关系',
    sourceTypes: ['hukou'],
    valueLabels: ['members'],
  },
  {
    fieldKey: 'account_name',
    label: '户名 / 账户名称',
    sourceTypes: ['account_license', 'bank_statement'],
    valueLabels: ['户名', '账户名称', '账户名', '存款人名称', 'account_name'],
  },
  {
    fieldKey: 'shareholder_names',
    label: '股东名称',
    sourceTypes: ['company_articles'],
    valueLabels: ['shareholders', '股东信息', '股东列表'],
  },
  {
    fieldKey: 'equity_ratios',
    label: '股权比例',
    sourceTypes: ['company_articles'],
    valueLabels: ['equity_ratios', '股权占比', 'shareholders'],
  },
];

const FIELD_CONSISTENCY_RULES = [
  'company_name',
  'credit_code',
  'legal_person',
  'registered_capital',
  'address',
  'bank_name',
  'account_number',
  'account_name',
  'shareholder_names',
  'equity_ratios',
] as const;

const FIELD_PRIORITY_RULES: Record<string, string[]> = {
  company_name: ['business_license', 'company_articles'],
  credit_code: ['business_license'],
  legal_person: ['business_license', 'company_articles'],
  registered_capital: ['business_license', 'company_articles'],
  address: ['business_license', 'company_articles'],
  bank_name: ['account_license', 'bank_statement'],
  account_number: ['account_license', 'bank_statement'],
  account_name: ['account_license', 'bank_statement'],
  shareholder_names: ['company_articles'],
  equity_ratios: ['company_articles'],
};

const FIELD_CONFLICT_PRIORITY_ORDER = [
  'account_number',
  'credit_code',
  'legal_person',
  'account_name',
  'bank_name',
  'registered_capital',
  'company_name',
  'address',
  'shareholder_names',
  'equity_ratios',
] as const;

const FIELD_ACTION_SUGGESTIONS: Record<string, string> = {
  company_name: '建议以营业执照为准，如存在变更请核对最新章程或补充工商变更资料。',
  credit_code: '建议优先核对营业执照原件或企业登记资料，确认统一社会信用代码后再继续后续流程。',
  legal_person: '建议以营业执照为准，如存在变更请核对工商信息。',
  registered_capital: '建议以营业执照为准，如章程金额不同建议人工核对出资情况。',
  address: '建议优先核对营业执照登记住所，如章程地址不同请确认是否存在迁址或未同步更新。',
  bank_name: '建议以开户许可证为准，如与对账单不一致建议核对银行账户信息。',
  account_number: '建议以开户许可证为准，如存在差异请重点核对账号准确性。',
  account_name: '建议以开户许可证为准，如与营业执照不一致建议人工核验。',
  shareholder_names: '建议结合公司章程及最新工商登记信息，确认股东名单是否存在变更或 OCR 识别遗漏。',
  equity_ratios: '建议优先核对公司章程中的出资额与注册资本计算结果，确认股权比例后再继续生成申请表或风控报告。',
};

const FIELD_IMPACT_TAGS: Record<string, string[]> = {
  company_name: ['影响申请表', '影响主体核验'],
  credit_code: ['影响申请表', '影响主体核验', '影响风控'],
  legal_person: ['影响申请表', '影响风控'],
  registered_capital: ['影响申请表', '影响风控'],
  address: ['影响申请表', '影响主体核验'],
  bank_name: ['影响账户核验', '影响风控'],
  account_number: ['影响账户核验', '影响风控'],
  account_name: ['影响账户核验', '影响风控'],
  shareholder_names: ['影响申请表', '影响控制权判断'],
  equity_ratios: ['影响申请表', '影响控制权判断', '影响风控'],
};

function getDocumentGroupKey(fileType: string): string {
  const normalized = String(fileType || '').trim();
  for (const groupKey of DOCUMENT_GROUP_ORDER) {
    if (DOCUMENT_GROUPS[groupKey].types.includes(normalized as never)) {
      return groupKey;
    }
  }
  return 'other';
}

function getDocumentGroupTitle(groupKey: string, fallbackTitle?: string): string {
  if (groupKey in DOCUMENT_GROUPS) {
    return DOCUMENT_GROUPS[groupKey as keyof typeof DOCUMENT_GROUPS].title;
  }
  return fallbackTitle || '其他资料';
}

function getDocumentTypeDisplayNameByCode(fileType: string): string {
  if (!fileType) {
    return '未分类资料';
  }
  const candidates = Object.values(DOCUMENT_GROUPS).flatMap((group) => group.types);
  const matchedType = candidates.find((item) => item === fileType);
  if (!matchedType) {
    return fileType;
  }
  const DISPLAY_NAMES: Record<string, string> = {
    business_license: '营业执照',
    company_articles: '公司章程',
    special_license: '特殊许可证',
    account_license: '开户许可证',
    bank_statement: '银行对账单',
    bank_statement_detail: '银行对账明细',
    id_card: '身份证',
    hukou: '户口本',
    marriage_cert: '结婚证',
    property_report: '房产证 / 产调',
    vehicle_license: '行驶证',
  };
  return DISPLAY_NAMES[fileType] || fileType;
}

function cleanMarkdownFieldValue(value: string): string {
  return value
    .replace(/\*\*/g, '')
    .replace(/`/g, '')
    .replace(/<br\s*\/?>/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/[，。,；;：:]+$/g, '');
}

function extractFieldValueFromMarkdown(markdown: string, labels: string[]): string {
  const lines = markdown.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = cleanMarkdownFieldValue(rawLine.replace(/^\s*[-*+]\s*/, ''));
    if (!line) {
      continue;
    }

    for (const label of labels) {
      const escapedLabel = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const match = line.match(new RegExp(`^${escapedLabel}\\s*[:：]\\s*(.+)$`, 'i'));
      if (match?.[1]) {
        return cleanMarkdownFieldValue(match[1]);
      }

      const looseMatch = line.match(new RegExp(`${escapedLabel}\\s*[:：]\\s*([^；;，,]+)`, 'i'));
      if (looseMatch?.[1]) {
        return cleanMarkdownFieldValue(looseMatch[1]);
      }
    }
  }
  return '';
}

function extractFieldValueFromLooseText(text: string, labels: string[]): string {
  const source = String(text || '');
  if (!source.trim()) {
    return '';
  }
  for (const label of labels) {
    const escapedLabel = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const match = source.match(new RegExp(`${escapedLabel}\\s*[:：]\\s*([^；;。\\n]+)`, 'i'));
    if (match?.[1]) {
      return cleanMarkdownFieldValue(match[1]);
    }
  }
  return '';
}

function extractCompanyNameFromLooseText(text: string): string {
  const source = String(text || '');
  if (!source.trim()) {
    return '';
  }
  const labeled = extractFieldValueFromLooseText(source, ['公司名称', '企业名称', '名称']);
  const labeledMatch = labeled.match(/([\u4e00-\u9fffA-Za-z0-9（）()·]+(?:有限责任公司|股份有限公司|有限公司|合伙企业))/);
  if (labeledMatch?.[1]) {
    return cleanMarkdownFieldValue(labeledMatch[1]);
  }
  const directMatch = source.match(/([\u4e00-\u9fffA-Za-z0-9（）()·]+(?:有限责任公司|股份有限公司|有限公司|合伙企业))/);
  return directMatch?.[1] ? cleanMarkdownFieldValue(directMatch[1]) : '';
}

function buildFieldSourceSummaries(
  markdown: string,
  documents: CustomerDocumentListItem[],
): FieldSourceSummary[] {
  return FIELD_SOURCE_RULES.map((rule) => {
    const sources = sortDocumentsWithinGroup(
      documents.filter((document) => rule.sourceTypes.includes(document.file_type)),
    );
    return {
      fieldKey: rule.fieldKey,
      label: rule.label,
      value: extractFieldValueFromMarkdown(markdown, rule.valueLabels),
      sources,
    };
  }).filter((item) => item.value || item.sources.length > 0);
}

function normalizeComparisonValue(fieldKey: string, value: string): string {
  const raw = cleanMarkdownFieldValue(value)
    .replace(/\u3000/g, ' ')
    .replace(/\r/g, '')
    .trim();
  const compact = raw.replace(/\s+/g, '');

  if (!compact) {
    return '';
  }

  if (fieldKey === 'account_number' || fieldKey === 'credit_code' || fieldKey === 'id_number') {
    return compact.replace(/[^0-9A-Za-z]/g, '').toUpperCase();
  }

  if (fieldKey === 'company_name' || fieldKey === 'account_name') {
    return compact.replace(/[，。、“”"'‘’（）()【】\[\]：:；;、,.]/g, '');
  }

  if (fieldKey === 'registered_capital') {
    const normalizedCapital = compact
      .replace(/^人民币/, '')
      .replace(/_/g, '')
      .replace(/万人民币/g, '万元')
      .replace(/万$/g, '万元');
    const amountMatch = normalizedCapital.match(/(\d+(?:\.\d+)?)(万元|元|亿)?/);
    if (!amountMatch) {
      return normalizedCapital;
    }
    const unit = amountMatch[2] || '';
    const amount = Number(amountMatch[1]);
    if (!Number.isFinite(amount)) {
      return normalizedCapital;
    }
    return `${amount.toFixed(4).replace(/\.?0+$/, '')}${unit}`;
  }

  if (fieldKey === 'address') {
    return compact.replace(/[，,。.;；]/g, '');
  }

  if (fieldKey === 'equity_ratios') {
    return compact
      .split(/[；;、]/)
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => {
        const match = item.match(/([\u4e00-\u9fffA-Za-z0-9·]+).*?(\d+(?:\.\d+)?)%/);
        if (match) {
          return `${match[1]}:${Number(match[2]).toFixed(2).replace(/\.?0+$/, '')}%`;
        }
        const decimalMatch = item.match(/([\u4e00-\u9fffA-Za-z0-9·]+).*?0\.(\d+)/);
        if (decimalMatch) {
          const ratio = Number(`0.${decimalMatch[2]}`) * 100;
          return `${decimalMatch[1]}:${ratio.toFixed(2).replace(/\.?0+$/, '')}%`;
        }
        return item;
      })
      .sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
      .join('|');
  }

  if (fieldKey === 'shareholder_names') {
    return compact
      .split(/[；;、,，]/)
      .map((item) => item.trim())
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
      .join('|');
  }

  return compact;
}

function stringifyExtractionValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return cleanMarkdownFieldValue(String(value));
  }
  if (Array.isArray(value)) {
    return cleanMarkdownFieldValue(value.map(stringifyExtractionValue).filter(Boolean).join('、'));
  }
  return cleanMarkdownFieldValue(JSON.stringify(value));
}

function isInvalidCompanyArticlesRoleValue(value: string): boolean {
  const text = String(value || '').trim();
  if (!text || text === '暂无') {
    return true;
  }

  const invalidFragments = [
    '姓名或者名称', '姓名或名称', '姓名名称', '姓名', '名称', '股东',
    '法定代表人', '执行董事', '董事长', '经理', '监事', '负责人',
    '信息', '资料', '说明', '无', '待定', '空白',
    '填写', '填报', '填入', '未填写', '未填报', '未填入',
    '签字', '签章', '盖章', '职务', '董事', '报酬', '及其报酬', '其报酬',
    '公司类型', '公司股东', '公司法', '决定聘任',
    '印章', '用章', '动用', '使用', '印鉴',
    '利润', '分配', '亏损', '利润分配', '弥补亏损',
    '委托', '受托', '国家', '机关', '授权',
    '报告', '通知', '通知书', '材料', '文件', '目录', '附件',
    '立本', '法规', '法律', '条例',
  ];

  if (invalidFragments.some((fragment) => text.includes(fragment))) {
    return true;
  }

  return !/^[\u4e00-\u9fff·]{2,4}$/.test(text);
}

function normalizeFieldLookupKey(value: string): string {
  return String(value || '').replace(/[\s_\-：:]/g, '').toLowerCase();
}

function flattenExtractionData(data: Record<string, unknown>, prefix = ''): Array<[string, unknown]> {
  return Object.entries(data).flatMap(([key, value]) => {
    const nextKey = prefix ? `${prefix}.${key}` : key;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      return [[nextKey, value] as [string, unknown], ...flattenExtractionData(value as Record<string, unknown>, nextKey)];
    }
    return [[nextKey, value] as [string, unknown]];
  });
}

function extractValueFromExtractionData(data: Record<string, unknown>, labels: string[]): string {
  const entries = flattenExtractionData(data);
  const normalizedLabels = labels.map(normalizeFieldLookupKey);

  const exactMatch = entries.find(([key, value]) => (
    normalizedLabels.includes(normalizeFieldLookupKey(key)) && stringifyExtractionValue(value)
  ));
  if (exactMatch) {
    return stringifyExtractionValue(exactMatch[1]);
  }

  const looseMatch = entries.find(([key, value]) => {
    const normalizedKey = normalizeFieldLookupKey(key);
    return normalizedLabels.some((label) => normalizedKey.endsWith(label) || normalizedKey.includes(label)) &&
      stringifyExtractionValue(value);
  });
  return looseMatch ? stringifyExtractionValue(looseMatch[1]) : '';
}

function extractConsistencyFieldValue(fieldKey: string, data: Record<string, unknown>, labels: string[]): string {
  if (fieldKey === 'personal_name') {
    const direct = extractValueFromExtractionData(data, labels);
    if (direct) {
      return direct;
    }
    const householdHead = stringifyExtractionValue(data.household_head_name || data['户主姓名']);
    if (householdHead) {
      return householdHead;
    }
  }

  if (fieldKey === 'spouse_name') {
    const persons = toRecordList(data.persons);
    const names = persons
      .map((item) => stringifyExtractionValue(item.name || item['姓名']))
      .filter(Boolean);
    if (names.length > 0) {
      return names.join('、');
    }
    const husbandName = stringifyExtractionValue(data.husband_name);
    const wifeName = stringifyExtractionValue(data.wife_name);
    return [husbandName, wifeName].filter(Boolean).join('、');
  }

  if (fieldKey === 'id_number') {
    const direct = extractValueFromExtractionData(data, labels);
    if (direct) {
      return direct;
    }
    const persons = toRecordList(data.persons);
    return persons
      .map((item) => stringifyExtractionValue(item.id_number || item['身份证号码'] || item['身份证号']))
      .filter(Boolean)
      .join('、');
  }

  if (fieldKey === 'household_member_names') {
    const members = toRecordList(data.members);
    const names = members
      .map((item) => stringifyExtractionValue(item.name || item['姓名']))
      .filter(Boolean);
    if (names.length > 0) {
      return names.join('、');
    }
    return stringifyExtractionValue(data.household_head_name || data['户主姓名']);
  }

  if (fieldKey === 'household_member_id_numbers') {
    const members = toRecordList(data.members);
    const idNumbers = members
      .map((item) => stringifyExtractionValue(item.id_number || item['身份证号码'] || item['身份证号']))
      .filter(Boolean);
    return idNumbers.join('、');
  }

  if (fieldKey === 'household_relationships') {
    const members = toRecordList(data.members);
    const relationPairs = members
      .map((item) => {
        const name = stringifyExtractionValue(item.name || item['姓名']);
        const relation = stringifyExtractionValue(item.relationship_to_head || item['与户主关系'] || item['户主或与户主关系']);
        if (!name && !relation) {
          return '';
        }
        return name && relation ? `${name} ${relation}` : (name || relation);
      })
      .filter(Boolean);
    return relationPairs.join('；');
  }

  if (fieldKey === 'shareholder_names') {
    const shareholders = toRecordList(data.shareholders);
    const names = shareholders
      .map((item) => stringifyExtractionValue(item.name || item.shareholder_name || item.shareholder))
      .filter(Boolean);
    return names.join('、');
  }

  if (fieldKey === 'equity_ratios') {
    const shareholders = toRecordList(data.shareholders);
    const ratioPairs = shareholders
      .map((item) => {
        const name = stringifyExtractionValue(item.name || item.shareholder_name || item.shareholder);
        const ratio = stringifyExtractionValue(item.equity_ratio || item.voting_ratio || item.ratio);
        if (!name || !ratio) {
          return '';
        }
        return `${name} ${ratio}`;
      })
      .filter(Boolean);
    if (ratioPairs.length > 0) {
      return ratioPairs.join('；');
    }
  }

  return extractValueFromExtractionData(data, labels);
}

function isDocumentTypeMatch(sourceType: string, targetType: string): boolean {
  const normalizedSource = normalizeFieldLookupKey(sourceType);
  const normalizedTarget = normalizeFieldLookupKey(targetType);
  const targetDisplayName = normalizeFieldLookupKey(getDocumentTypeDisplayNameByCode(targetType));
  return normalizedSource === normalizedTarget || normalizedSource === targetDisplayName;
}

function getDocumentsByType(documents: CustomerDocumentListItem[], documentType: string): CustomerDocumentListItem[] {
  return sortDocumentsWithinGroup(documents.filter((document) => isDocumentTypeMatch(document.file_type, documentType)));
}

function getDocumentTypeDisplayLabel(
  document: CustomerDocumentListItem,
  extractionGroups: ExtractionGroup[],
): string {
  const defaultLabel = document.file_type_name || getDocumentTypeDisplayNameByCode(document.file_type);
  if (document.file_type !== 'id_card') {
    return defaultLabel;
  }

  const matchedExtraction = extractionGroups
    .filter((group) => (group.extraction_type || '') === 'id_card')
    .flatMap((group) => group.items || [])
    .find((item) => String((item as ExtractionItem & { doc_id?: string }).doc_id || '').trim() === document.doc_id);

  const extractedData = matchedExtraction?.extracted_data as Record<string, unknown> | undefined;
  const idName = extractValueFromExtractionData(extractedData || {}, ['name', '姓名']).trim();
  if (!idName) {
    return defaultLabel;
  }
  return `${defaultLabel}（${idName}）`;
}

function getExtractionItemsByType(groups: ExtractionGroup[], documentType: string): ExtractionItem[] {
  const matchedGroups = groups.filter((group) => isDocumentTypeMatch(group.extraction_type, documentType));
  return matchedGroups
    .flatMap((group) => group.items)
    .sort((a, b) => new Date(b.created_at || '').getTime() - new Date(a.created_at || '').getTime());
}

function getFirstValidFieldValueByTypes(
  extractionGroups: ExtractionGroup[],
  documentTypes: string[],
  fieldLabels: string[],
): string {
  for (const documentType of documentTypes) {
    const items = getExtractionItemsByType(extractionGroups, documentType);
    for (const item of items) {
      const extractedData = (item.extracted_data || {}) as Record<string, unknown>;
      const value = extractValueFromExtractionData(extractedData, fieldLabels);
      if (value) {
        return value;
      }
    }
  }
  return '';
}

function hasCompleteIdCardExtraction(extractionGroups: ExtractionGroup[]): boolean {
  const items = getExtractionItemsByType(extractionGroups, 'id_card');
  return items.some((item) => {
    const extractedData = (item.extracted_data || {}) as Record<string, unknown>;
    const name = extractValueFromExtractionData(extractedData, ['name', '姓名']);
    const idNumber = extractValueFromExtractionData(extractedData, ['id_number', '身份证号码', '身份证号']);
    return Boolean(name && idNumber);
  });
}

function hasUsableHukouExtraction(extractionGroups: ExtractionGroup[]): boolean {
  const items = getExtractionItemsByType(extractionGroups, 'hukou');
  return items.some((item) => {
    const extractedData = (item.extracted_data || {}) as Record<string, unknown>;
    const headName = extractValueFromExtractionData(extractedData, ['household_head_name', '户主姓名']);
    const householdAddress = extractValueFromExtractionData(extractedData, ['household_address', '户籍地址', 'address', '地址']);
    const members = extractedData.members;
    const memberCount = Array.isArray(members) ? members.length : 0;
    return Boolean((headName || memberCount > 0) && householdAddress);
  });
}

function hasUsableMarriageCertExtraction(extractionGroups: ExtractionGroup[]): boolean {
  const items = getExtractionItemsByType(extractionGroups, 'marriage_cert');
  return items.some((item) => {
    const extractedData = (item.extracted_data || {}) as Record<string, unknown>;
    const husbandName = extractValueFromExtractionData(extractedData, ['husband_name']);
    const wifeName = extractValueFromExtractionData(extractedData, ['wife_name']);
    const registrationDate = extractValueFromExtractionData(extractedData, ['registration_date', '结婚登记日期', '登记日期']);
    const persons = toRecordList(extractedData.persons);
    const personNames = persons
      .map((person) => stringifyExtractionValue(person.name || person['姓名']))
      .filter(Boolean);
    return Boolean((husbandName && wifeName) || personNames.length >= 2 || registrationDate);
  });
}

function buildFieldConsistencyResults(
  documents: CustomerDocumentListItem[],
  extractionGroups: ExtractionGroup[],
): FieldConsistencyResult[] {
  return FIELD_CONSISTENCY_RULES.map((fieldKey) => {
    const rule = FIELD_SOURCE_RULES.find((item) => item.fieldKey === fieldKey);
    if (!rule) {
      return null;
    }

    const comparedSources = rule.sourceTypes.flatMap((documentType) => {
      const typeDocuments = getDocumentsByType(documents, documentType);
      const extractionItems = getExtractionItemsByType(extractionGroups, documentType);
      return extractionItems.flatMap((item, index) => {
        const rawValue = extractConsistencyFieldValue(
          fieldKey,
          (item.extracted_data || {}) as Record<string, unknown>,
          rule.valueLabels,
        );
        if (!rawValue) {
          return [];
        }
        const document = typeDocuments[index] || typeDocuments[0];
        const documentTypeName = document?.file_type_name || getDocumentTypeDisplayNameByCode(documentType);
        return [{
          extractionId: item.extraction_id,
          documentType,
          documentTypeName,
          fileName: document?.file_name || documentTypeName,
          rawValue,
          normalizedValue: normalizeComparisonValue(fieldKey, rawValue),
          document,
        }];
      });
    });

    const priorityTypes = FIELD_PRIORITY_RULES[fieldKey] || [];
    const nonEmptySources = comparedSources
      .filter((source) => source.normalizedValue)
      .sort((a, b) => {
        const aIndex = priorityTypes.findIndex((type) => isDocumentTypeMatch(a.documentType, type));
        const bIndex = priorityTypes.findIndex((type) => isDocumentTypeMatch(b.documentType, type));
        const normalizedA = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
        const normalizedB = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;
        if (normalizedA !== normalizedB) {
          return normalizedA - normalizedB;
        }
        const aTime = new Date(a.document?.upload_time || '').getTime();
        const bTime = new Date(b.document?.upload_time || '').getTime();
        return bTime - aTime;
      });
    const uniqueValues = new Set(nonEmptySources.map((source) => source.normalizedValue));
    const status: FieldConsistencyStatus = nonEmptySources.length < 2
      ? 'insufficient'
      : uniqueValues.size === 1
        ? 'consistent'
        : 'conflict';

    return {
      fieldKey,
      label: rule.label,
      status,
      comparedSources: nonEmptySources,
    };
  }).filter(Boolean) as FieldConsistencyResult[];
}

function formatConsistencyValueForDisplay(fieldKey: string, value: string): string[] {
  const cleanValue = cleanMarkdownFieldValue(value);
  if (!cleanValue) {
    return [];
  }

  if (fieldKey === 'shareholder_names') {
    return cleanValue
      .split(/[；;、]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  if (fieldKey === 'equity_ratios') {
    return cleanValue
      .split(/[；;]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  return [cleanValue];
}

function getFieldConflictPriority(fieldKey: string): number {
  const index = FIELD_CONFLICT_PRIORITY_ORDER.indexOf(fieldKey as (typeof FIELD_CONFLICT_PRIORITY_ORDER)[number]);
  return index === -1 ? Number.MAX_SAFE_INTEGER : index;
}

function getFieldImpactTags(fieldKey: string): string[] {
  return FIELD_IMPACT_TAGS[fieldKey] || [];
}

function toRecordList(value: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object' && !Array.isArray(item));
}

function toStringList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => stringifyExtractionValue(item))
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildCompanyArticlesInsight(
  documents: CustomerDocumentListItem[],
  extractionGroups: ExtractionGroup[],
): CompanyArticlesInsight | null {
  const latestItem = getExtractionItemsByType(extractionGroups, 'company_articles')[0];
  if (!latestItem) {
    return null;
  }

  const extractedData = (latestItem.extracted_data || {}) as Record<string, unknown>;
  const document = getDocumentsByType(documents, 'company_articles')[0];
  const summaryText = stringifyExtractionValue(extractedData.summary);
  const rawLegalPerson = stringifyExtractionValue(extractedData.legal_person);
  const fallbackLegalPerson = getFirstValidFieldValueByTypes(
    extractionGroups,
    ['business_license', 'account_license'],
    ['legal_person', '法定代表人', '负责人'],
  );
  const legalPerson = !isInvalidCompanyArticlesRoleValue(rawLegalPerson)
    ? rawLegalPerson
    : (!isInvalidCompanyArticlesRoleValue(fallbackLegalPerson) ? fallbackLegalPerson : '');
  const executiveDirector = stringifyExtractionValue(extractedData.executive_director);
  const chairman = stringifyExtractionValue(extractedData.chairman);
  const manager = stringifyExtractionValue(extractedData.manager);
  const supervisor = stringifyExtractionValue(extractedData.supervisor);
  const sanitizedExecutiveDirector = isInvalidCompanyArticlesRoleValue(executiveDirector) ? '' : executiveDirector;
  const sanitizedChairman = isInvalidCompanyArticlesRoleValue(chairman) ? '' : chairman;
  const sanitizedManager = isInvalidCompanyArticlesRoleValue(manager) ? '' : manager;
  const sanitizedSupervisor = isInvalidCompanyArticlesRoleValue(supervisor) ? '' : supervisor;
  const managementRolesSummaryParts = [
    legalPerson ? `法定代表人：${legalPerson}` : '',
    sanitizedExecutiveDirector ? `执行董事：${sanitizedExecutiveDirector}` : '',
    sanitizedChairman ? `董事长：${sanitizedChairman}` : '',
    sanitizedManager ? `经理：${sanitizedManager}` : '',
    sanitizedSupervisor ? `监事：${sanitizedSupervisor}` : '',
  ].filter(Boolean);

  const fallbackCompanyName = getFirstValidFieldValueByTypes(
    extractionGroups,
    ['business_license'],
    ['company_name', '公司名称', '企业名称', '名称'],
  );
  const fallbackBusinessScope = getFirstValidFieldValueByTypes(
    extractionGroups,
    ['business_license'],
    ['business_scope', '经营范围'],
  );
  const fallbackAddress = getFirstValidFieldValueByTypes(
    extractionGroups,
    ['business_license'],
    ['address', '住所', '地址'],
  );
  const companyName = stringifyExtractionValue(extractedData.company_name)
    || fallbackCompanyName
    || extractCompanyNameFromLooseText(summaryText);
  const businessScope = stringifyExtractionValue(extractedData.business_scope)
    || extractFieldValueFromLooseText(summaryText, ['经营范围', '经营项目'])
    || fallbackBusinessScope;
  const address = stringifyExtractionValue(extractedData.address)
    || extractFieldValueFromLooseText(summaryText, ['地址', '住所'])
    || fallbackAddress;
  const managementStructure = stringifyExtractionValue(extractedData.management_structure)
    || extractFieldValueFromLooseText(summaryText, ['治理结构', '组织机构'])
    || (managementRolesSummaryParts.length > 0 ? '股东会、执行董事（或董事长）、监事、经理' : '');

    return {
      companyName,
      registeredCapital: stringifyExtractionValue(extractedData.registered_capital),
      businessScope,
      address,
      managementStructure,
      summary: summaryText,
      legalPerson,
      executiveDirector: sanitizedExecutiveDirector,
      chairman: sanitizedChairman,
      manager: sanitizedManager,
      supervisor: sanitizedSupervisor,
      managementRolesSummary: managementRolesSummaryParts.join('；'),
      managementRoleEvidenceLines: toStringList(extractedData.management_role_evidence_lines),
      shareholderCount: stringifyExtractionValue(extractedData.shareholder_count),
      equityStructureSummary: stringifyExtractionValue(extractedData.equity_structure_summary),
      shareholders: toRecordList(extractedData.shareholders),
      financingApprovalRule: stringifyExtractionValue(extractedData.financing_approval_rule),
    financingApprovalThreshold: stringifyExtractionValue(extractedData.financing_approval_threshold),
    majorDecisionRuleDetails: toRecordList(extractedData.major_decision_rule_details),
    document,
  };
}

function buildHukouInsightFromRawPages(rawPagesValue: unknown, rawTextValue?: unknown): HukouInsight | null {
  const rawPages = toRecordList(rawPagesValue)
    .map((item) => ({
      page: Number(item.page || 0),
      text: stringifyExtractionValue(item.text),
    }))
    .filter((item) => item.text);
  const rawText = stringifyExtractionValue(rawTextValue);
  const synthesizedPages = rawPages.length > 0
    ? rawPages
    : rawText
      ? rawText
          .split(/---\s*第\s*\d+\s*页\s*---/)
          .map((text, index) => ({ page: index + 1, text: text.trim() }))
          .filter((item) => item.text)
      : [];
  if (synthesizedPages.length === 0) {
    return null;
  }

  const cleanLines = (text: string): string[] =>
    text
      .split(/\r?\n/)
      .map((line) => line.replace(/\s+/g, ' ').trim())
      .filter(Boolean);

  const knownLabels = [
    '户别', '户主姓名', '户号', '住址', '户籍地址', '地址', '登记机关', '签发机关',
    '常住人口登记卡', '姓名', '户主或与户主关系', '与户主关系', '性别', '民族', '出生日期',
    '公民身份号码', '身份证号码', '身份证号', '婚姻状况', '兵役状况', '服务处所', '职业', '籍贯', '出生地',
  ];

  const normalizeInlineValue = (value: string): string =>
    value
      .replace(/^[：:\s]+/, '')
      .replace(/[|｜]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

  const isKnownLabelLine = (line: string): boolean =>
    knownLabels.some((label) => line === label || line.startsWith(`${label}：`) || line.startsWith(`${label}:`));

  const findLabelValue = (
    lines: string[],
    labels: string[],
    stopLabels: string[] = [],
    maxLookahead = 8,
    validator?: (value: string) => boolean,
  ): string => {
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      for (const label of labels) {
        const inlinePattern = new RegExp(`^${label}\\s*[:：]?\\s*(.+)$`);
        const inlineMatch = line.match(inlinePattern);
        if (inlineMatch) {
          const candidate = normalizeInlineValue(inlineMatch[1]);
          if (candidate && !stopLabels.some((stopLabel) => candidate.includes(stopLabel)) && (!validator || validator(candidate))) {
            return candidate;
          }
        }
        if (line === label || line.includes(label)) {
          for (let offset = 1; offset <= maxLookahead; offset += 1) {
            const candidate = normalizeInlineValue(lines[index + offset] || '');
            if (!candidate) {
              break;
            }
            if (stopLabels.some((stopLabel) => candidate.includes(stopLabel)) || isKnownLabelLine(candidate)) {
              break;
            }
            if (!validator || validator(candidate)) {
              return candidate;
            }
          }
        }
      }
    }
    return '';
  };

  const isReasonableName = (value: string): boolean => {
    const text = value.trim();
    if (!text || text.length > 8) {
      return false;
    }
    if (['姓名', '户主或与', '公民身份号码', '住址', '常住人口登记卡', '登记事项', '户口', '调查'].some((fragment) => text.includes(fragment))) {
      return false;
    }
    return /^[\u4e00-\u9fff·]{2,8}$/.test(text);
  };

  const relationshipOptions = ['户主', '妻', '夫', '子', '女', '父', '母', '配偶', '长子', '长女', '次子', '次女', '孙子', '孙女'];
  const isRelationshipValue = (value: string): boolean => relationshipOptions.includes(value.trim());
  const isGenderValue = (value: string): boolean => ['男', '女'].includes(value.trim());
  const isEthnicityValue = (value: string): boolean => /^[\u4e00-\u9fff]{1,8}$/.test(value.trim()) && !value.includes('出生') && !value.includes('日期');
  const isBirthDateValue = (value: string): boolean => /(?:19|20)\d{2}年\d{2}月\d{2}日/.test(value.trim());
  const isIdNumberValue = (value: string): boolean => /\d{17}[\dXx]/.test(value.trim());
  const isMaritalStatusValue = (value: string): boolean => ['已婚', '未婚', '有配偶', '无配偶', '离婚', '离异', '丧偶', '再婚'].includes(value.trim());

  const homepage = {
    householdHeadName: '',
    householdNumber: '',
    householdType: '',
    householdAddress: '',
    registrationAuthority: '',
  };
  const members: Array<Record<string, unknown>> = [];
  const seenMemberKeys = new Set<string>();
  let homepagePresent = false;

  for (const page of synthesizedPages) {
    const lines = cleanLines(page.text);
    if (lines.length === 0) {
      continue;
    }
    const joined = lines.join('\n');
    const homepageHits = ['户别', '户主姓名', '户号', '住址'].filter((label) => joined.includes(label)).length;
    if (homepageHits >= 3) {
      const headName = findLabelValue(lines, ['户主姓名'], ['户号', '户别', '住址', '登记事项变更'], 6, isReasonableName)
        || (joined.match(/户主姓名\s*[:：]?\s*([\u4e00-\u9fff·]{2,8})/)?.[1] || '');
      const pageHomepage = {
        householdHeadName: isReasonableName(headName) ? headName : '',
        householdNumber: findLabelValue(lines, ['户号'], ['户别', '住址', '户主姓名'], 6, (value) => /^[A-Za-z0-9]{4,}$/.test(value)) || (joined.match(/户号\s*[:：]?\s*([A-Za-z0-9]+)/)?.[1] || ''),
        householdType: findLabelValue(lines, ['户别'], ['户号', '住址', '户主姓名'], 6, (value) => value.length >= 2 && value.length <= 20) || (joined.match(/户别\s*[:：]?\s*([^\n]{2,20})/)?.[1] || ''),
        householdAddress: findLabelValue(lines, ['住址', '户籍地址', '地址'], ['户主姓名', '户号', '户别', '登记事项变更', '常住人口登记卡'], 8, (value) => value.length >= 4)
          || (joined.match(/住址\s*[:：]?\s*([^\n]{4,80})/)?.[1] || ''),
        registrationAuthority: lines.find((line) => line.includes('派出所') || line.includes('公安局')) || '',
      };
      if (Object.values(pageHomepage).some(Boolean)) {
        homepagePresent = true;
      }
      if (!homepage.householdHeadName && pageHomepage.householdHeadName) homepage.householdHeadName = pageHomepage.householdHeadName;
      if (!homepage.householdNumber && pageHomepage.householdNumber) homepage.householdNumber = pageHomepage.householdNumber;
      if (!homepage.householdType && pageHomepage.householdType) homepage.householdType = pageHomepage.householdType;
      if (!homepage.householdAddress && pageHomepage.householdAddress) homepage.householdAddress = pageHomepage.householdAddress;
      if (!homepage.registrationAuthority && pageHomepage.registrationAuthority) homepage.registrationAuthority = pageHomepage.registrationAuthority;
    }

    const memberHits = ['常住人口登记卡', '姓名', '户主或与户主关系', '公民身份号码', '出生日期'].filter((label) => joined.includes(label)).length;
    if (memberHits >= 2) {
      let name = (
        findLabelValue(lines, ['姓名'], ['户主或与户主关系', '公民身份号码', '性别', '民族', '出生日期'], 6, isReasonableName)
        || (joined.match(/姓名\s*[:：]?\s*([\u4e00-\u9fff·]{2,8})/)?.[1] || '')
      ).trim();
      if (!name) {
        const idx = lines.indexOf('姓名');
        if (idx >= 0 && lines[idx + 1] && isReasonableName(lines[idx + 1])) {
          name = lines[idx + 1];
        }
      }
      const relationship = (
        findLabelValue(lines, ['户主或与户主关系', '与户主关系'], ['公民身份号码', '性别', '民族', '出生日期', '婚姻状况', '服务处所', '职业'], 6, isRelationshipValue)
        || (joined.match(/户主或与户主关系\s*[:：]?\s*(户主|妻|夫|子|女|父|母|配偶|长子|长女|次子|次女|孙子|孙女)/)?.[1] || '')
      ).trim();
      const member = {
        name: isReasonableName(name) ? name : '',
        relationship_to_head: isRelationshipValue(relationship) ? relationship : (relationship.startsWith('户主') ? '户主' : ''),
        gender: (findLabelValue(lines, ['性别'], ['民族', '出生日期', '公民身份号码', '婚姻状况'], 4, isGenderValue) || (joined.match(/性别\s*[:：]?\s*(男|女)/)?.[1] || '')).trim(),
        ethnicity: (findLabelValue(lines, ['民族'], ['出生日期', '公民身份号码', '婚姻状况'], 4, isEthnicityValue) || (joined.match(/民族\s*[:：]?\s*([\u4e00-\u9fff]{1,8})/)?.[1] || '')).trim(),
        birth_date: (findLabelValue(lines, ['出生日期'], ['公民身份号码', '婚姻状况', '服务处所', '职业'], 4, isBirthDateValue) || (joined.match(/((?:19|20)\d{2}年\d{2}月\d{2}日)/)?.[1] || '')).trim(),
        id_number: (findLabelValue(lines, ['公民身份号码', '身份证号码', '身份证号'], ['婚姻状况', '服务处所', '职业'], 4, isIdNumberValue) || (joined.match(/\d{17}[\dXx]/)?.[0] || '')).toUpperCase(),
        marital_status: (findLabelValue(lines, ['婚姻状况'], ['服务处所', '职业', '兵役状况', '何时由何地迁来本市（县）', '何时由何地迁来本址'], 4, isMaritalStatusValue) || (joined.match(/婚姻状况\s*[:：]?\s*(已婚|未婚|有配偶|无配偶|离婚|离异|丧偶|再婚)/)?.[1] || '')).trim(),
      } as Record<string, unknown>;
      if (!member.birth_date && typeof member.id_number === 'string' && member.id_number.length >= 14) {
        const raw = member.id_number.slice(6, 14);
        if (/^\d{8}$/.test(raw)) {
          member.birth_date = `${raw.slice(0, 4)}年${raw.slice(4, 6)}月${raw.slice(6, 8)}日`;
        }
      }
      const key = stringifyExtractionValue(member.id_number) || `${stringifyExtractionValue(member.name)}|${stringifyExtractionValue(member.birth_date)}`.replace(/^\|+|\|+$/g, '');
      if ((member.name || member.id_number) && key && !seenMemberKeys.has(key)) {
        seenMemberKeys.add(key);
        members.push(member);
      }
    }
  }

  if (!homepage.householdHeadName) {
    const headMember = members.find((member) => stringifyExtractionValue(member.relationship_to_head) === '户主');
    homepage.householdHeadName = stringifyExtractionValue(headMember?.name);
  }

  const completenessNote = homepagePresent && members.length > 0
    ? '已识别户口本首页和成员页'
    : homepagePresent
      ? '已识别户口本首页，缺少成员页'
      : members.length > 0
        ? '已识别成员页，缺少户口本首页'
        : '户口本信息不完整';

  return {
    householdHeadName: homepage.householdHeadName,
    householdNumber: homepage.householdNumber,
    householdType: homepage.householdType,
    householdAddress: homepage.householdAddress,
    registrationAuthority: homepage.registrationAuthority,
    completenessNote,
    members,
  };
}

function buildHukouInsight(
  documents: CustomerDocumentListItem[],
  extractionGroups: ExtractionGroup[],
): HukouInsight | null {
  const latestItem = getExtractionItemsByType(extractionGroups, 'hukou')[0];
  if (!latestItem) {
    return null;
  }

  const extractedData = (latestItem.extracted_data || {}) as Record<string, unknown>;
  const document = getDocumentsByType(documents, 'hukou')[0];
  const parsedInsight = buildHukouInsightFromRawPages(extractedData.raw_pages, extractedData.raw_text);
  const isMeaningfulValue = (value: string): boolean => {
    const text = value.trim();
    return Boolean(text && text !== '暂无' && text !== '—' && text !== '-');
  };
  const extractedMembers = toRecordList(extractedData.members).filter((item) => {
    const name = stringifyExtractionValue(item.name || item['姓名']);
    const idNumber = stringifyExtractionValue(item.id_number || item['身份证号码'] || item['身份证号']);
    return Boolean(name || idNumber);
  });
  const mergedMembers = new Map<string, Record<string, unknown>>();
  [...(parsedInsight?.members || []), ...extractedMembers].forEach((item) => {
    const key = stringifyExtractionValue(item.id_number || item['身份证号码'] || item['身份证号'])
      || `${stringifyExtractionValue(item.name || item['姓名'])}|${stringifyExtractionValue(item.birth_date || item['出生日期'])}`.replace(/^\|+|\|+$/g, '');
    if (!key) {
      return;
    }
    const existing = mergedMembers.get(key);
    if (!existing) {
      mergedMembers.set(key, { ...item });
      return;
    }
    const next = { ...existing };
    Object.entries(item).forEach(([field, value]) => {
      const currentValue = stringifyExtractionValue(next[field]);
      const incomingValue = stringifyExtractionValue(value);
      if (!isMeaningfulValue(currentValue) && isMeaningfulValue(incomingValue)) {
        next[field] = value;
      }
    });
    mergedMembers.set(key, next);
  });
  const members = Array.from(mergedMembers.values());

  const extractedHeadName = extractValueFromExtractionData(extractedData, ['household_head_name', '户主姓名']);
  let householdHeadName = isMeaningfulValue(extractedHeadName) ? extractedHeadName : '';
  if (!householdHeadName) {
    const householdHeadMember = members.find((item) => stringifyExtractionValue(item.relationship_to_head || item['与户主关系'] || item['户主或与户主关系']) === '户主');
    householdHeadName = stringifyExtractionValue(householdHeadMember?.name || householdHeadMember?.['姓名']);
  }

  const extractedHouseholdNumber = extractValueFromExtractionData(extractedData, ['household_number', '户号']);
  const extractedHouseholdType = extractValueFromExtractionData(extractedData, ['household_type', '户别']);
  const extractedHouseholdAddress = extractValueFromExtractionData(extractedData, ['household_address', '户籍地址', '住址', '地址']);
  const extractedRegistrationAuthority = extractValueFromExtractionData(extractedData, ['registration_authority', '登记机关', '签发机关']);
  const extractedCompletenessNote = extractValueFromExtractionData(extractedData, ['completeness_note', '完整性提示']);

  return {
    householdHeadName: householdHeadName || parsedInsight?.householdHeadName || '',
    householdNumber: (isMeaningfulValue(extractedHouseholdNumber) ? extractedHouseholdNumber : '') || parsedInsight?.householdNumber || '',
    householdType: (isMeaningfulValue(extractedHouseholdType) ? extractedHouseholdType : '') || parsedInsight?.householdType || '',
    householdAddress: (isMeaningfulValue(extractedHouseholdAddress) ? extractedHouseholdAddress : '') || parsedInsight?.householdAddress || '',
    registrationAuthority: (isMeaningfulValue(extractedRegistrationAuthority) ? extractedRegistrationAuthority : '') || parsedInsight?.registrationAuthority || '',
    completenessNote: (isMeaningfulValue(extractedCompletenessNote) ? extractedCompletenessNote : '') || parsedInsight?.completenessNote || '',
    members,
    document,
  };
}

function buildHukouMembersMarkdown(members: Array<Record<string, unknown>>): string {
  if (members.length === 0) {
    return '### 家庭成员\n- 暂未识别到成员信息';
  }

  const header = [
    '### 家庭成员',
    '| 姓名 | 与户主关系 | 性别 | 民族 | 出生日期 | 身份证号码 | 婚姻状况 |',
    '|---|---|---|---|---|---|---|',
  ];

  const rows = members.map((member) => {
    const values = [
      stringifyExtractionValue(member.name || member['姓名']),
      stringifyExtractionValue(member.relationship_to_head || member['与户主关系'] || member['户主或与户主关系']),
      stringifyExtractionValue(member.gender || member['性别']),
      stringifyExtractionValue(member.ethnicity || member['民族']),
      stringifyExtractionValue(member.birth_date || member['出生日期']),
      stringifyExtractionValue(member.id_number || member['身份证号码'] || member['身份证号']),
      stringifyExtractionValue(member.marital_status || member['婚姻状况']),
    ].map((value) => value.trim() || '—');
    return `| ${values.join(' | ')} |`;
  });

  return [...header, ...rows].join('\n');
}

function synchronizeHukouMarkdown(markdown: string, insight: HukouInsight | null): string {
  if (!markdown || !insight) {
    return markdown;
  }

  const sectionMatch = markdown.match(/## 户口本[\s\S]*?(?=\n##\s|$)/);
  if (!sectionMatch) {
    return markdown;
  }

  const section = sectionMatch[0];
  const fileName = insight.document?.file_name || '暂无';
  const originalStatus = insight.document
    ? (insight.document.original_available ? '可查看' : '仅保留提取结果')
    : '暂无';
  const nextSection = [
    '## 户口本',
    `- 资料类型：户口本`,
    `- 来源文件：${fileName}`,
    `- 原件状态：${originalStatus}`,
    '',
    '### 结构化提取结果',
    `- 户主姓名：${insight.householdHeadName || '暂无'}`,
    `- 户号：${insight.householdNumber || '暂无'}`,
    `- 户别：${insight.householdType || '暂无'}`,
    `- 户籍地址：${insight.householdAddress || '暂无'}`,
    `- 登记机关：${insight.registrationAuthority || '暂无'}`,
    `- 完整性提示：${insight.completenessNote || '暂无'}`,
    '',
    buildHukouMembersMarkdown(insight.members),
  ].join('\n');

  return markdown.replace(section, nextSection);
}

function synchronizeCompanyArticlesMarkdown(
  markdown: string,
  insight: CompanyArticlesInsight | null,
  equityRatioSummary: string,
): string {
  if (!markdown || !insight) {
    return markdown;
  }

  const sectionMatch = markdown.match(/## 公司章程[\s\S]*?(?=\n##\s|$)/);
  if (!sectionMatch) {
    return markdown;
  }

  const section = sectionMatch[0];
  const hasMeaningfulValue = (value: string): boolean => Boolean(value && value.trim() && value.trim() !== '暂无');

  const applyLine = (content: string, label: string, value: string): string => {
    if (!hasMeaningfulValue(value)) {
      return content;
    }
    const nextLine = `- ${label}：${value}`;
    const pattern = new RegExp(`^- ${label}：.*$`, 'm');
    if (pattern.test(content)) {
      return content.replace(pattern, nextLine);
    }
    return content;
  };

  const legalPersonLine = `- 法定代表人：${insight.legalPerson || '暂无'}`;
  const summaryLine = insight.managementRolesSummary ? `- 任职信息摘要：${insight.managementRolesSummary}` : '';
  const equityRatioLine = equityRatioSummary ? `- 股权占比：${equityRatioSummary}` : '';

  let nextSection = section;
  nextSection = applyLine(nextSection, '公司名称', insight.companyName);
  nextSection = applyLine(nextSection, '注册资本', insight.registeredCapital);
  nextSection = applyLine(nextSection, '地址', insight.address);
  nextSection = applyLine(nextSection, '治理结构', insight.managementStructure);
  nextSection = applyLine(nextSection, '摘要', insight.summary);
  if (/^- 经营范围：.*$/m.test(nextSection)) {
    nextSection = nextSection.replace(/^- 经营范围：.*$\n?/m, '');
  }
  if (/^- 法定代表人：.*$/m.test(nextSection)) {
    nextSection = nextSection.replace(/^- 法定代表人：.*$/m, legalPersonLine);
  }

  if (/^- 任职信息摘要：.*$/m.test(nextSection)) {
    if (summaryLine) {
      nextSection = nextSection.replace(/^- 任职信息摘要：.*$/m, summaryLine);
    } else {
      nextSection = nextSection.replace(/^- 任职信息摘要：.*$\n?/m, '');
    }
  } else if (summaryLine) {
    if (/^- 监事：.*$/m.test(nextSection)) {
      nextSection = nextSection.replace(/(^- 监事：.*$)/m, `$1\n${summaryLine}`);
    } else if (/^- 法定代表人：.*$/m.test(nextSection)) {
      nextSection = nextSection.replace(/(^- 法定代表人：.*$)/m, `$1\n${summaryLine}`);
    }
  }

  if (/^- 股权占比：.*$/m.test(nextSection)) {
    if (equityRatioLine) {
      nextSection = nextSection.replace(/^- 股权占比：.*$/m, equityRatioLine);
    } else {
      nextSection = nextSection.replace(/^- 股权占比：.*$\n?/m, '');
    }
  } else if (equityRatioLine) {
    if (/^- 股权结构：.*$/m.test(nextSection)) {
      nextSection = nextSection.replace(/(^- 股权结构：.*$)/m, `$1\n${equityRatioLine}`);
    } else if (/^- 股东数量：.*$/m.test(nextSection)) {
      nextSection = nextSection.replace(/(^- 股东数量：.*$)/m, `$1\n${equityRatioLine}`);
    }
  }

  return markdown.replace(section, nextSection);
}

function parseRatioNumber(value: string): number | null {
  const match = String(value || '').match(/(\d+(?:\.\d+)?)/);
  if (!match) {
    return null;
  }
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseAmountNumber(value: string): number | null {
  const normalized = String(value || '').replace(/人民币/g, '').replace(/,/g, '').trim();
  const match = normalized.match(/(\d+(?:\.\d+)?)/);
  if (!match) {
    return null;
  }
  const parsed = Number(match[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatComputedRatio(value: number): string {
  const rounded = Math.round(value * 100) / 100;
  return Number.isInteger(rounded) ? `${rounded}%` : `${rounded.toFixed(2)}%`;
}

function buildCompanyArticlesShareholderViews(
  shareholders: Array<Record<string, unknown>>,
  registeredCapital: string,
): CompanyArticlesShareholderView[] {
  const capitalNumber = parseAmountNumber(registeredCapital);
  const items = shareholders.map((shareholder) => {
    const contribution = stringifyExtractionValue(shareholder.capital_contribution);
    const rawRatio = stringifyExtractionValue(shareholder.equity_ratio);
    const contributionNumber = parseAmountNumber(contribution);
    const computedRatio = !rawRatio && capitalNumber && contributionNumber
      ? formatComputedRatio((contributionNumber / capitalNumber) * 100)
      : '';
    const ratio = rawRatio || computedRatio;
    return {
      name: stringifyExtractionValue(shareholder.name),
      contribution,
      method: stringifyExtractionValue(shareholder.contribution_method),
      date: stringifyExtractionValue(shareholder.contribution_date),
      ratio,
      ratioNumber: parseRatioNumber(ratio),
      isPrimary: false,
    };
  });

  const sorted = [...items].sort((a, b) => {
    const aRatio = a.ratioNumber ?? -1;
    const bRatio = b.ratioNumber ?? -1;
    if (aRatio !== bRatio) {
      return bRatio - aRatio;
    }
    return a.name.localeCompare(b.name, 'zh-CN');
  });

  const maxRatio = sorted.reduce<number | null>((current, item) => {
    if (item.ratioNumber === null) {
      return current;
    }
    if (current === null || item.ratioNumber > current) {
      return item.ratioNumber;
    }
    return current;
  }, null);

  return sorted.map((item) => ({
    ...item,
    isPrimary: maxRatio !== null && item.ratioNumber !== null && item.ratioNumber === maxRatio,
  }));
}

function buildCompanyArticlesEquityRatioSummary(shareholders: CompanyArticlesShareholderView[]): string {
  const parts = shareholders
    .filter((item) => item.name && item.ratio)
    .map((item) => `${item.name} ${item.ratio}`);
  return parts.join('；');
}

function buildCompanyArticlesRuleDetailViews(details: Array<Record<string, unknown>>): CompanyArticlesRuleDetailView[] {
  return details.map((item) => ({
    topic: stringifyExtractionValue(item.topic),
    rule: stringifyExtractionValue(item.rule),
    threshold: stringifyExtractionValue(item.threshold),
  }));
}

function buildCompanyArticlesControlSummary(
  shareholders: CompanyArticlesShareholderView[],
): CompanyArticlesControlSummary | null {
  if (shareholders.length === 0) {
    return null;
  }

  const validShareholders = shareholders.filter((item) => item.ratioNumber !== null);
  if (validShareholders.length === 0) {
    return null;
  }

  const [first, second] = validShareholders;
  if (first && first.ratioNumber !== null && first.ratioNumber >= 50) {
    return {
      label: '控股股东',
      description: `${first.name} 当前占股 ${first.ratio}，已达到控股股东判断阈值。`,
    };
  }

  if (
    first && second &&
    first.ratioNumber !== null && second.ratioNumber !== null &&
    first.ratioNumber + second.ratioNumber > 50
  ) {
    return {
      label: '共同控制候选',
      description: `${first.name} 与 ${second.name} 合计占股 ${first.ratioNumber + second.ratioNumber}%，建议结合章程表决规则继续核对是否属于共同控制。`,
    };
  }

  return {
    label: '股权分散',
    description: '当前未看到单一股东超过 50%，建议结合表决规则和一致行动安排继续判断控制关系。',
  };
}

function buildCompanyArticlesRuleGroups(
  items: CompanyArticlesRuleDetailView[],
): CompanyArticlesRuleGroupView[] {
  const groups = new Map<string, CompanyArticlesRuleDetailView[]>();
  items.forEach((item) => {
    const topic = item.topic || '其他事项';
    const current = groups.get(topic) || [];
    current.push(item);
    groups.set(topic, current);
  });
  return Array.from(groups.entries()).map(([topic, groupItems]) => ({ topic, items: groupItems }));
}

function getRuleTopicBadgeClass(topic: string): string {
  const normalized = String(topic || '');
  if (normalized.includes('融资')) {
    return 'border-blue-200 bg-blue-50 text-blue-700';
  }
  if (normalized.includes('贷款') || normalized.includes('借款')) {
    return 'border-cyan-200 bg-cyan-50 text-cyan-700';
  }
  if (normalized.includes('担保')) {
    return 'border-rose-200 bg-rose-50 text-rose-700';
  }
  if (normalized.includes('章程')) {
    return 'border-violet-200 bg-violet-50 text-violet-700';
  }
  if (normalized.includes('股权')) {
    return 'border-amber-200 bg-amber-50 text-amber-700';
  }
  return 'border-slate-200 bg-slate-50 text-slate-700';
}

function getCompanyArticlesSourceLabel(document?: CustomerDocumentListItem): string {
  if (!document) {
    return '来源：公司章程结构化提取';
  }
  const fileName = document.file_name || '公司章程';
  return `来源：${fileName}`;
}

function getPrioritySource(
  fieldKey: string,
  sources: FieldConsistencySourceValue[],
): FieldConsistencySourceValue | null {
  const priorityTypes = FIELD_PRIORITY_RULES[fieldKey] || [];
  for (const type of priorityTypes) {
    const matched = sources.find((source) => isDocumentTypeMatch(source.documentType, type));
    if (matched) {
      return matched;
    }
  }
  return sources[0] || null;
}

function getActionSuggestion(fieldKey: string): string {
  return FIELD_ACTION_SUGGESTIONS[fieldKey] || '建议人工核对相关资料。';
}

function getFieldConsistencyStatusMeta(status: FieldConsistencyStatus): { label: string; className: string; cardClassName: string } {
  if (status === 'consistent') {
    return {
      label: '一致',
      className: 'border-emerald-200 bg-emerald-50 text-emerald-700',
      cardClassName: 'border-emerald-100 bg-emerald-50/70',
    };
  }
  if (status === 'conflict') {
    return {
      label: '存在差异',
      className: 'border-orange-200 bg-orange-50 text-orange-700',
      cardClassName: 'border-orange-100 bg-orange-50/70',
    };
  }
  return {
    label: '信息不足',
    className: 'border-slate-200 bg-slate-100 text-slate-600',
    cardClassName: 'border-slate-200 bg-slate-50',
  };
}

function getCompletenessStatusMeta(status: CompletenessStatus): { label: string; className: string } {
  if (status === 'complete') {
    return {
      label: '已完整',
      className: 'border-emerald-200 bg-emerald-50 text-emerald-700',
    };
  }
  if (status === 'pending') {
    return {
      label: '待补充',
      className: 'border-amber-200 bg-amber-50 text-amber-700',
    };
  }
  if (status === 'available') {
    return {
      label: '已有资料',
      className: 'border-sky-200 bg-sky-50 text-sky-700',
    };
  }
  return {
    label: '暂无资料',
    className: 'border-slate-200 bg-slate-100 text-slate-600',
  };
}

function getReadinessStatusMeta(status: ReadinessStatus): { label: string; className: string; cardClassName: string } {
  if (status === 'ready') {
    return {
      label: '已满足',
      className: 'border-emerald-200 bg-emerald-50 text-emerald-700',
      cardClassName: 'border-emerald-100 bg-emerald-50/70',
    };
  }
  if (status === 'suggest') {
    return {
      label: '建议补充',
      className: 'border-amber-200 bg-amber-50 text-amber-700',
      cardClassName: 'border-amber-100 bg-amber-50/70',
    };
  }
  return {
    label: '暂不满足',
    className: 'border-red-200 bg-red-50 text-red-700',
    cardClassName: 'border-red-100 bg-red-50/70',
  };
}

function uniqueLabels(labels: string[]): string[] {
  return Array.from(new Set(labels.filter(Boolean)));
}

function getOverallReadinessSummary(cards: CompletenessCard[]): ReadinessSummary {
  const byKey = new Map(cards.map((card) => [card.key, card]));
  const enterprise = byKey.get('enterprise');
  const banking = byKey.get('banking');
  const personal = byKey.get('personal');
  const asset = byKey.get('asset');

  const enterpriseReady = enterprise?.status === 'complete';
  const bankingReady = banking?.status === 'complete';
  const personalReady = personal?.status === 'complete';
  const hasBankingSupplement = Boolean(
    banking?.existingLabels.some((label) => ['银行对账单', '银行对账明细'].includes(label))
  );
  const hasPersonalSupplement = Boolean(personal && personal.existingLabels.length > 0);
  const hasAssetSupplement = Boolean(asset && asset.existingLabels.length > 0);
  const hasAnySupplement = hasBankingSupplement || hasPersonalSupplement || hasAssetSupplement;

  const applicationMissing = uniqueLabels([
    ...(enterprise?.missingRequiredLabels ?? []),
    ...(banking?.missingRequiredLabels ?? []),
  ]);
  const applicationReady = enterpriseReady && bankingReady;

  const matchingMissing = uniqueLabels([
    ...applicationMissing,
    ...(!hasAnySupplement ? ['银行对账单', '身份证或资产资料'] : []),
  ]);
  const matchingReady = applicationReady && hasAnySupplement;
  const matchingStatus: ReadinessStatus = matchingReady ? 'ready' : applicationReady ? 'suggest' : 'blocked';

  const riskMissing = uniqueLabels([
    ...applicationMissing,
    ...(personal?.missingRequiredLabels ?? []),
  ]);
  const riskReady = enterpriseReady && bankingReady && personalReady;

  const items: ReadinessItem[] = [
    {
      key: 'application',
      title: '申请表基础生成',
      status: applicationReady ? 'ready' : 'blocked',
      message: applicationReady
        ? '当前资料已满足申请表基础生成条件。'
        : `当前资料暂不满足申请表基础生成条件，缺少：${applicationMissing.join('、') || '关键资料'}。`,
      missingLabels: applicationMissing,
    },
    {
      key: 'matching',
      title: '方案匹配',
      status: matchingStatus,
      message: matchingReady
        ? '当前资料可进入方案匹配。'
        : applicationReady
          ? '当前资料已满足基础条件，但建议补充银行对账单、个人或资产类资料后再做方案匹配。'
          : `当前资料暂不建议直接进入方案匹配，建议补充：${matchingMissing.join('、') || '关键资料'}。`,
      missingLabels: matchingMissing,
      suggestion: matchingReady ? '可继续补充银行对账明细以提高匹配准确度。' : undefined,
    },
    {
      key: 'risk',
      title: '风控判断',
      status: riskReady ? 'ready' : 'blocked',
      message: riskReady
        ? '当前资料已满足风控判断最低要求。'
        : `当前资料暂不满足风控判断最低要求，缺少：${riskMissing.join('、') || '关键资料'}。`,
      missingLabels: riskMissing,
    },
  ];

  const missingLabels = uniqueLabels(items.flatMap((item) => item.missingLabels));
  const actions: ActionType[] = [];
  if (applicationReady) {
    actions.push('generate_application');
  }
  if (matchingReady) {
    actions.push('scheme_match');
  }
  if (!applicationReady || !riskReady || matchingStatus === 'suggest') {
    actions.push('upload_missing');
  }

  const overallStatus: ReadinessStatus = items.every((item) => item.status === 'ready')
    ? 'ready'
    : applicationReady
      ? 'suggest'
      : 'blocked';

  return {
    overallStatus,
    overallTitle:
      overallStatus === 'ready'
        ? '当前资料已具备主要业务流转条件'
        : overallStatus === 'suggest'
          ? '当前资料可先推进部分流程，建议继续补充'
          : '当前资料暂不建议直接进入关键流程',
    overallDescription:
      overallStatus === 'ready'
        ? '申请表、方案匹配和风控判断的最低资料要求均已满足。'
        : overallStatus === 'suggest'
          ? '申请表基础条件已满足，但方案匹配或风控判断仍建议补充资料以提高准确度。'
          : `请先补充关键资料：${missingLabels.join('、') || '营业执照、开户许可证、身份证'}。`,
    items,
    actions: uniqueLabels(actions) as ActionType[],
    missingLabels,
  };
}

function getActionLabel(action: ActionType): string {
  if (action === 'generate_application') {
    return '去生成申请表';
  }
  if (action === 'scheme_match') {
    return '去方案匹配';
  }
  return '去上传缺失资料';
}

function buildActionPath(action: ActionType, customerId: string, missingLabels: string[]): string {
  const encodedCustomerId = encodeURIComponent(customerId);
  const missingParam = missingLabels.length > 0
    ? `&missing=${encodeURIComponent(missingLabels.join(','))}`
    : '';
  if (action === 'generate_application') {
    return `/application?customer_id=${encodedCustomerId}`;
  }
  if (action === 'scheme_match') {
    return `/scheme?customer_id=${encodedCustomerId}`;
  }
  return `/upload?customer_id=${encodedCustomerId}${missingParam}`;
}

function clickExistingNavigation(page: 'application' | 'scheme' | 'upload'): boolean {
  const pageIndex: Record<'upload' | 'application' | 'scheme', number> = {
    upload: 2,
    application: 4,
    scheme: 5,
  };
  const navButtons = Array.from(document.querySelectorAll<HTMLButtonElement>('aside nav button'));
  const targetButton = navButtons[pageIndex[page]];
  if (!targetButton) {
    return false;
  }
  targetButton.click();
  return true;
}

function sortDocumentsWithinGroup(items: CustomerDocumentListItem[]): CustomerDocumentListItem[] {
  return [...items].sort((a, b) => {
    if (a.is_latest !== b.is_latest) {
      return a.is_latest ? -1 : 1;
    }
    const aTime = new Date(a.upload_time || '').getTime();
    const bTime = new Date(b.upload_time || '').getTime();
    if (!Number.isNaN(aTime) && !Number.isNaN(bTime) && aTime !== bTime) {
      return bTime - aTime;
    }
    return String(b.file_name || '').localeCompare(String(a.file_name || ''), 'zh-CN');
  });
}

function sanitizeProfileMarkdown(markdown: string): string {
  const invalidLegalPersonValues = [
    '姓名或者名称',
    '姓名或名称',
    '姓名名称',
    '信息',
    '资料',
    '说明',
    '无',
    '暂无',
    '待定',
    '空白',
    '填写',
    '填报',
    '填入',
    '未填写',
    '未填报',
    '未填入',
    '职务',
    '董事',
      '报酬',
      '及其报酬',
      '其报酬',
      '公司类型',
      '公司股东',
      '公司法',
      '决定聘任',
      '印章',
      '用章',
      '动用',
      '使用',
      '制度',
      '印鉴',
      '利润',
      '分配',
      '亏损',
      '利润分配',
      '弥补亏损',
      '委托',
      '受托',
      '国家',
      '机关',
      '授权',
      '报告',
      '通知',
      '通知书',
      '材料',
      '文件',
      '目录',
      '附件',
      '立本',
      '法规',
      '法律',
      '条例',
      '签字',
    '签章',
    '盖章',
    '姓名',
    '名称',
    '股东',
    '法定代表人',
    '的法定代表人',
    '执行董事',
    '的执行董事',
    '董事长',
    '的董事长',
    '负责人',
    '的负责人',
    '事)担任。',
  ];

    const roleLabels = ['法定代表人', '执行董事', '董事长', '经理', '监事'];
    const sanitizedRoleMarkdown = roleLabels.reduce((roleCurrent, roleLabel) => (
      invalidLegalPersonValues.reduce((current, value) => (
        current.replace(new RegExp(`(- ${roleLabel}：)\\s*${value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`, 'gm'), '$1暂无')
      ), roleCurrent)
    ), markdown);
    const sanitizedRoleFragmentMarkdown = roleLabels.reduce((current, roleLabel) => (
      current.replace(
        new RegExp(`(- ${roleLabel}：)[^\\n]*(制度|印章|用章|动用|使用|印鉴)[^\\n]*`, 'g'),
        '$1暂无'
      )
    ), sanitizedRoleMarkdown);
    const sanitizedBusinessFragmentMarkdown = roleLabels.reduce((current, roleLabel) => (
      current.replace(
        new RegExp(`(- ${roleLabel}：)[^\\n]*(利润|分配|亏损|收益|财务|会计|清算|章程|事项|委托|受托|国家|机关|授权|报告|通知|材料|文件|目录|附件|立本|法规|法律|条例)[^\\n]*`, 'g'),
        '$1暂无'
      )
    ), sanitizedRoleFragmentMarkdown);

    return markdown
      ? sanitizedBusinessFragmentMarkdown
      .replace(/^>.*customer_id=.*$/gm, '')
    .replace(/^- 客户ID：.*$/gm, '')
    .replace(/(- 客户类型：)\s*enterprise\b/g, '$1企业')
    .replace(/(- 客户类型：)\s*personal\b/g, '$1个人')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
    : '';
}

function formatProfileDateTime(value?: string | null): string {
  if (!value) {
    return '未记录';
  }

  const normalized = value.includes('T')
    ? value
    : value.includes(' ')
      ? `${value.replace(' ', 'T')}Z`
      : value;
  const date = new Date(normalized);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString('zh-CN', {
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

const CustomerDataPage: React.FC<CustomerDataPageProps> = ({ onBack }) => {
  const { state, setCurrentCustomer, recordSystemActivity } = useApp();
  const [customers, setCustomers] = useState<CustomerListItem[]>([]);
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(state.extraction.currentCustomerId);
  const [profile, setProfile] = useState<CustomerProfileMarkdownResponse | null>(null);
  const [draft, setDraft] = useState('');
  const [mode, setMode] = useState<EditorMode>('edit');
  const [loadingCustomers, setLoadingCustomers] = useState(true);
  const [loadingProfile, setLoadingProfile] = useState(false);
  const [loadingDocuments, setLoadingDocuments] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [customerSearch, setCustomerSearch] = useState('');
  const [documentFilter, setDocumentFilter] = useState<DocumentFilterMode>('all');
  const [documents, setDocuments] = useState<CustomerDocumentListItem[]>([]);
  const [extractionGroups, setExtractionGroups] = useState<ExtractionGroup[]>([]);
  const [collapsedDocumentGroups, setCollapsedDocumentGroups] = useState<Record<string, boolean>>({});
  const [showConsistentFields, setShowConsistentFields] = useState(false);
  const [focusedConflictDocId, setFocusedConflictDocId] = useState('');
  const [focusedConflictDocIds, setFocusedConflictDocIds] = useState<string[]>([]);
  const [focusedConflictLabel, setFocusedConflictLabel] = useState('');
  const [error, setError] = useState<string | null>(null);
  const urlParams = useMemo(() => new URLSearchParams(window.location.search), []);
  const customerIdFromUrl = urlParams.get('customer_id') || urlParams.get('customerId') || '';
  const highlightDocId = urlParams.get('highlight_doc_id') || '';
  const highlightDocumentType = urlParams.get('highlight_document_type') || '';
  const highlightFileName = urlParams.get('highlight_file_name') || '';

  const clearCustomerUrlParams = useCallback(() => {
    const url = new URL(window.location.href);
    url.searchParams.delete('customer_id');
    url.searchParams.delete('customerId');
    url.searchParams.delete('highlight_doc_id');
    url.searchParams.delete('highlight_document_type');
    url.searchParams.delete('highlight_file_name');
    window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
  }, []);

  const clearCurrentCustomerData = useCallback(() => {
    setSelectedCustomerId(null);
    setProfile(null);
    setDraft('');
    setDocuments([]);
    setExtractionGroups([]);
    setCurrentCustomer(null, null);
  }, [setCurrentCustomer]);

  const loadCustomers = useCallback(async () => {
    setLoadingCustomers(true);
    setError(null);
    try {
      const items = await listCustomers();
      const allowedCustomerIds = new Set(items.map((item) => item.record_id).filter(Boolean));
      const urlCustomerId = customerIdFromUrl.trim();
      setCustomers(items);

      if (urlCustomerId && !allowedCustomerIds.has(urlCustomerId)) {
        clearCustomerUrlParams();
        clearCurrentCustomerData();
        return;
      }

      setSelectedCustomerId((current) => {
        if (urlCustomerId) {
          return urlCustomerId;
        }
        if (current && items.some((item) => item.record_id === current)) {
          return current;
        }
        if (current) {
          return null;
        }
        return items[0]?.record_id || null;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载客户列表失败');
      clearCurrentCustomerData();
    } finally {
      setLoadingCustomers(false);
    }
  }, [clearCurrentCustomerData, clearCustomerUrlParams, customerIdFromUrl]);

  const loadProfile = useCallback(
    async (customerId: string) => {
      setLoadingProfile(true);
      setError(null);
      setProfile(null);
      setDraft('');
      try {
        const result = await getCustomerProfileMarkdown(customerId);
        const sanitizedMarkdown = sanitizeProfileMarkdown(result.markdown_content);
        setProfile(result);
        setDraft(sanitizedMarkdown);
        const matchedCustomer = customers.find((item) => item.record_id === customerId);
        setCurrentCustomer(matchedCustomer?.name ?? result.customer_name, customerId);
      } catch (err) {
        setError(err instanceof Error ? err.message : '加载资料汇总失败');
        setProfile(null);
        setDraft('');
      } finally {
        setLoadingProfile(false);
      }
    },
    [customers, setCurrentCustomer]
  );

  const loadDocuments = useCallback(async (customerId: string) => {
    setLoadingDocuments(true);
    setDocuments([]);
    try {
      const result = await getCustomerDocuments(customerId);
      setDocuments(result);
    } catch (err) {
      setDocuments([]);
      setError(err instanceof Error ? err.message : '加载资料文件列表失败');
    } finally {
      setLoadingDocuments(false);
    }
  }, []);

  const loadExtractions = useCallback(async (customerId: string) => {
    setExtractionGroups([]);
    try {
      const result = await getCustomerExtractions(customerId);
      setExtractionGroups(result);
    } catch (err) {
      setExtractionGroups([]);
      setError(err instanceof Error ? err.message : '加载结构化提取结果失败');
    }
  }, []);

  useEffect(() => {
    void loadCustomers();
  }, [loadCustomers]);

  useEffect(() => {
    if (!customerIdFromUrl) {
      return;
    }
    if (loadingCustomers) {
      return;
    }
    const allowedCustomerIds = new Set(customers.map((item) => item.record_id));
    if (!allowedCustomerIds.has(customerIdFromUrl)) {
      clearCustomerUrlParams();
      clearCurrentCustomerData();
      return;
    }
    setSelectedCustomerId((current) => (current === customerIdFromUrl ? current : customerIdFromUrl));
  }, [clearCurrentCustomerData, clearCustomerUrlParams, customerIdFromUrl, customers, loadingCustomers]);

  useEffect(() => {
    if (!selectedCustomerId) {
      setProfile(null);
      setDraft('');
      setDocuments([]);
      setExtractionGroups([]);
      setCurrentCustomer(null, null);
      return;
    }
    if (loadingCustomers) return;
    const allowedCustomerIds = new Set(customers.map((item) => item.record_id));
    if (!allowedCustomerIds.has(selectedCustomerId)) {
      clearCustomerUrlParams();
      clearCurrentCustomerData();
      return;
    }
    void loadProfile(selectedCustomerId);
    void loadDocuments(selectedCustomerId);
    void loadExtractions(selectedCustomerId);
  }, [
    clearCurrentCustomerData,
    clearCustomerUrlParams,
    customers,
    loadingCustomers,
    selectedCustomerId,
    loadDocuments,
    loadExtractions,
    loadProfile,
    setCurrentCustomer,
  ]);

  const selectedCustomer = useMemo(
    () => customers.find((item) => item.record_id === selectedCustomerId) ?? null,
    [customers, selectedCustomerId]
  );
  const filteredCustomers = useMemo(() => {
    const keyword = customerSearch.trim().toLowerCase();
    if (!keyword) {
      return customers;
    }
    return customers.filter((item) => {
      const name = (item.name || '').toLowerCase();
      const type = (item.customer_type || '').toLowerCase();
      return name.includes(keyword) || type.includes(keyword);
    });
  }, [customerSearch, customers]);

  const filteredDocuments = useMemo(() => {
    let nextDocuments = documents;
    if (documentFilter === 'original') {
      nextDocuments = nextDocuments.filter((item) => item.original_available);
    }
    if (documentFilter === 'extraction') {
      nextDocuments = nextDocuments.filter((item) => !item.original_available);
    }
    if (focusedConflictDocIds.length > 0) {
      const idSet = new Set(focusedConflictDocIds);
      nextDocuments = nextDocuments.filter((item) => idSet.has(item.doc_id));
    }
    return nextDocuments;
  }, [documentFilter, documents, focusedConflictDocIds]);

  const groupedDocuments = useMemo(() => {
    const groups = new Map<
      string,
      {
        key: string;
        title: string;
        items: CustomerDocumentListItem[];
        latestCount: number;
        originalCount: number;
      }
    >();

    filteredDocuments.forEach((document) => {
      const key = getDocumentGroupKey(document.file_type);
      const existing = groups.get(key);
      if (existing) {
        existing.items.push(document);
        if (document.is_latest) existing.latestCount += 1;
        if (document.original_available) existing.originalCount += 1;
        return;
      }

      groups.set(key, {
        key,
        title: getDocumentGroupTitle(key, document.file_type_name || document.file_type || '未分类资料'),
        items: [document],
        latestCount: document.is_latest ? 1 : 0,
        originalCount: document.original_available ? 1 : 0,
      });
    });

    const orderedGroups = Array.from(groups.values()).map((group) => ({
      ...group,
      items: sortDocumentsWithinGroup(group.items),
    }));

    return orderedGroups.sort((a, b) => {
      const aIndex = DOCUMENT_GROUP_ORDER.indexOf(a.key as (typeof DOCUMENT_GROUP_ORDER)[number]);
      const bIndex = DOCUMENT_GROUP_ORDER.indexOf(b.key as (typeof DOCUMENT_GROUP_ORDER)[number]);
      const normalizedA = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
      const normalizedB = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;
      if (normalizedA !== normalizedB) {
        return normalizedA - normalizedB;
      }
      return a.title.localeCompare(b.title, 'zh-CN');
    });
  }, [filteredDocuments]);

  useEffect(() => {
    setCollapsedDocumentGroups((current) => {
      const next = { ...current };
      groupedDocuments.forEach((group) => {
        if (!(group.key in next)) {
          next[group.key] = false;
        }
      });
      Object.keys(next).forEach((key) => {
        if (!groupedDocuments.some((group) => group.key === key)) {
          delete next[key];
        }
      });
      return next;
    });
  }, [groupedDocuments]);

  const isHighlightedDocument = useCallback(
    (document: CustomerDocumentListItem): boolean => {
      if (highlightDocId && document.doc_id === highlightDocId) {
        return true;
      }
      if (highlightFileName && document.file_name === highlightFileName) {
        return true;
      }
      if (highlightDocumentType && document.file_type === highlightDocumentType) {
        return true;
      }
      return false;
    },
    [highlightDocId, highlightDocumentType, highlightFileName],
  );

  useEffect(() => {
    if (!selectedCustomerId) {
      return;
    }
    const highlightedDoc = documents.find((item) => isHighlightedDocument(item));
    if (!highlightedDoc) {
      return;
    }
    const timer = window.setTimeout(() => {
      const target = document.querySelector<HTMLElement>(`[data-doc-id="${highlightedDoc.doc_id}"]`);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, 350);
    return () => window.clearTimeout(timer);
  }, [documents, isHighlightedDocument, selectedCustomerId]);

  const originalDocumentCount = useMemo(() => documents.filter((item) => item.original_available).length, [documents]);
  const extractionOnlyDocumentCount = useMemo(
    () => documents.filter((item) => !item.original_available).length,
    [documents]
  );
  const completenessCards = useMemo<CompletenessCard[]>(() => {
    const presentTypes = new Set(documents.map((item) => item.file_type).filter(Boolean));
    const idCardComplete = hasCompleteIdCardExtraction(extractionGroups);
    const hukouComplete = hasUsableHukouExtraction(extractionGroups);
    const marriageCertComplete = hasUsableMarriageCertExtraction(extractionGroups);

    return DOCUMENT_GROUP_ORDER.map((groupKey) => {
      const rule = DOCUMENT_COMPLETENESS_RULES[groupKey];
      const existingRequired = rule.required.filter((type) => presentTypes.has(type));
      const existingOptional = rule.optional.filter((type) => presentTypes.has(type));
      const missingRequired = rule.required.filter((type) => !presentTypes.has(type));
      const missingOptional = rule.optional.filter((type) => !presentTypes.has(type));
      const existingTypes = [...existingRequired, ...existingOptional];
      const idCardIncomplete = groupKey === 'personal' && presentTypes.has('id_card') && !idCardComplete;
      const hukouIncomplete = groupKey === 'personal' && presentTypes.has('hukou') && !hukouComplete;
      const marriageCertIncomplete = groupKey === 'personal' && presentTypes.has('marriage_cert') && !marriageCertComplete;

      let status: CompletenessStatus = 'pending';
      if (groupKey === 'asset') {
        status = existingTypes.length > 0 ? 'available' : 'empty';
      } else if (idCardIncomplete) {
        status = 'pending';
      } else if (rule.required.length > 0 && missingRequired.length === 0) {
        status = 'complete';
      } else if (existingTypes.length === 0) {
        status = 'pending';
      } else {
        status = 'pending';
      }

      return {
        key: groupKey,
        title: rule.title,
        description: rule.description,
        status,
        existingLabels: existingTypes.map((type) => (
          type === 'id_card' && idCardIncomplete
            ? '身份证（信息不完整）'
            : type === 'hukou' && hukouIncomplete
              ? '户口本（信息不完整）'
            : type === 'marriage_cert' && marriageCertIncomplete
              ? '结婚证（信息不完整）'
            : getDocumentTypeDisplayNameByCode(type)
        )),
        missingRequiredLabels: [
          ...missingRequired.map(getDocumentTypeDisplayNameByCode),
          ...(idCardIncomplete ? ['身份证关键信息（姓名、身份证号码）'] : []),
        ],
        missingOptionalLabels: [
          ...missingOptional.map(getDocumentTypeDisplayNameByCode),
          ...(hukouIncomplete ? ['户口本关键信息（户主姓名/成员信息、户籍地址）'] : []),
          ...(marriageCertIncomplete ? ['结婚证关键信息（双方姓名或登记日期）'] : []),
        ],
      };
    });
  }, [documents, extractionGroups]);
  const readinessSummary = useMemo(
    () => getOverallReadinessSummary(completenessCards),
    [completenessCards]
  );
  const companyArticlesInsight = useMemo(
    () => buildCompanyArticlesInsight(documents, extractionGroups),
    [documents, extractionGroups]
  );
  const hukouInsight = useMemo(
    () => buildHukouInsight(documents, extractionGroups),
    [documents, extractionGroups]
  );
  const companyArticlesRoleItems = useMemo(
    () => {
      const items = [
        { label: '法定代表人', value: companyArticlesInsight?.legalPerson || '暂无' },
        { label: '执行董事', value: companyArticlesInsight?.executiveDirector || '' },
        { label: '董事长', value: companyArticlesInsight?.chairman || '' },
        { label: '经理', value: companyArticlesInsight?.manager || '' },
        { label: '监事', value: companyArticlesInsight?.supervisor || '' },
      ];
      return items.filter((item) => item.label === '法定代表人' || (item.value && item.value !== '暂无'));
    },
    [companyArticlesInsight]
  );
  const companyArticlesShareholderViews = useMemo(
    () => buildCompanyArticlesShareholderViews(
      companyArticlesInsight?.shareholders || [],
      companyArticlesInsight?.registeredCapital || '',
    ),
    [companyArticlesInsight]
  );
  const companyArticlesEquityRatioSummary = useMemo(
    () => buildCompanyArticlesEquityRatioSummary(companyArticlesShareholderViews),
    [companyArticlesShareholderViews]
  );
  const renderedDraft = useMemo(
    () => synchronizeHukouMarkdown(
      synchronizeCompanyArticlesMarkdown(draft, companyArticlesInsight, companyArticlesEquityRatioSummary),
      hukouInsight,
    ),
    [draft, companyArticlesInsight, companyArticlesEquityRatioSummary, hukouInsight]
  );
  const fieldSourceSummaries = useMemo(
    () => buildFieldSourceSummaries(renderedDraft, documents),
    [documents, renderedDraft]
  );
  const companyArticlesControlSummary = useMemo(
    () => buildCompanyArticlesControlSummary(companyArticlesShareholderViews),
    [companyArticlesShareholderViews]
  );
  const companyArticlesRuleDetailViews = useMemo(
    () => buildCompanyArticlesRuleDetailViews(companyArticlesInsight?.majorDecisionRuleDetails || []),
    [companyArticlesInsight]
  );
  const companyArticlesRuleGroups = useMemo(
    () => buildCompanyArticlesRuleGroups(companyArticlesRuleDetailViews),
    [companyArticlesRuleDetailViews]
  );
  const fieldConsistencyResults = useMemo(
    () => buildFieldConsistencyResults(documents, extractionGroups),
    [documents, extractionGroups]
  );
  const consistentConsistencyResults = useMemo(
    () => fieldConsistencyResults.filter((result) => result.status === 'consistent'),
    [fieldConsistencyResults]
  );
  const conflictingConsistencyResults = useMemo(
    () => fieldConsistencyResults
      .filter((result) => result.status === 'conflict')
      .sort((a, b) => {
        const aPriority = getFieldConflictPriority(a.fieldKey);
        const bPriority = getFieldConflictPriority(b.fieldKey);
        if (aPriority !== bPriority) {
          return aPriority - bPriority;
        }
        return a.label.localeCompare(b.label, 'zh-CN');
      }),
    [fieldConsistencyResults]
  );
  const insufficientConsistencyResults = useMemo(
    () => fieldConsistencyResults
      .filter((result) => result.status === 'insufficient')
      .sort((a, b) => getFieldConflictPriority(a.fieldKey) - getFieldConflictPriority(b.fieldKey)),
    [fieldConsistencyResults]
  );

  const profileStatusLabel = profile?.source_mode === 'manual' ? '手动整理中' : '系统已整理';
  const profileStatusClassName =
    profile?.source_mode === 'manual'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
      : 'border-sky-200 bg-sky-50 text-sky-700';
  const profileVersionLabel = profile?.version ? `V${profile.version}` : 'V1';
  const profileHintText =
    profile?.source_mode === 'manual'
      ? '当前版本会优先用于资料问答与报告生成'
      : '当前内容由系统整理生成，可继续手动修订';

  const isDirty = draft !== (profile ? sanitizeProfileMarkdown(profile.markdown_content) : '');
  const profileFeedback = useMemo(() => {
    if (!selectedCustomerId) {
      return {
        tone: 'idle' as const,
        title: '等待选择客户',
        description: '请先选择客户，再查看、编辑或保存资料汇总。',
        persistenceHint: '尚未进入资料汇总处理。',
        nextStep: '先从左侧选择客户，再继续整理资料。',
      };
    }

    if (loadingProfile) {
      return {
        tone: 'processing' as const,
        title: '正在加载资料汇总',
        description: '系统正在读取当前客户的资料汇总内容与版本信息。',
        persistenceHint: '主流程处理中。',
        nextStep: '请稍候，加载完成后可继续查看或编辑。',
      };
    }

    if (saving) {
      return {
        tone: 'processing' as const,
        title: '正在保存资料汇总',
        description: '系统正在保存当前修改，并同步更新资料问答与风险评估使用的资料版本。',
        persistenceHint: '主流程处理中，保存完成后会立即生效。',
        nextStep: '请稍候，保存完成后建议去 AI 对话验证最新内容。',
      };
    }

    if (saveSuccess) {
      return {
        tone: 'success' as const,
        title: '资料汇总已保存',
        description: '当前客户资料汇总已经更新，资料问答和风险报告会优先读取这份最新版本。',
        persistenceHint: '主流程已保存成功。',
        nextStep: '建议前往 AI 对话验证资料问答或重新生成风险报告。',
      };
    }

    if (error) {
      return {
        tone: 'error' as const,
        title: '资料汇总处理失败',
        description: error,
        persistenceHint: profile ? '本次修改未保存，上一版资料汇总仍可继续使用。' : '当前没有保存成功的新版本。',
        nextStep: '请检查内容后重试，或先刷新当前客户资料。',
      };
    }

    if (isDirty) {
      return {
        tone: 'partial' as const,
        title: '检测到未保存修改',
        description: '你已经修改了当前资料汇总，但系统仍在使用上一版已保存内容。',
        persistenceHint: '主流程仍使用上一版已保存资料。',
        nextStep: '确认无误后点击保存，再去资料问答或风险报告查看变化。',
      };
    }

    return {
      tone: 'idle' as const,
      title: '资料汇总已就绪',
      description: '当前客户资料汇总可以继续查看、修订和预览。',
      persistenceHint: '当前展示的是系统可用版本。',
      nextStep: '如需更新内容，可直接编辑并保存。',
    };
  }, [selectedCustomerId, loadingProfile, saving, saveSuccess, error, profile, isDirty]);

  const handleSave = useCallback(async () => {
    if (!selectedCustomerId) return;
    setSaving(true);
    setSaveSuccess(false);
    setError(null);
    try {
      const result = await updateCustomerProfileMarkdown(selectedCustomerId, {
        markdown_content: renderedDraft,
        title: selectedCustomer?.name ? `${selectedCustomer.name} 资料汇总` : undefined,
      });
      setProfile(result);
      setDraft(sanitizeProfileMarkdown(result.markdown_content));
      recordSystemActivity({
        type: 'profile',
        title: '资料汇总已更新',
        description: '系统已保存最新资料整理内容，并同步刷新资料问答索引。',
        customerName: selectedCustomer?.name ?? result.customer_name,
        customerId: selectedCustomerId,
        status: 'success',
      });
      setSaveSuccess(true);
      window.setTimeout(() => setSaveSuccess(false), 2200);
    } catch (err) {
      setError(err instanceof Error ? err.message : '保存资料汇总失败');
    } finally {
      setSaving(false);
    }
  }, [draft, recordSystemActivity, selectedCustomer, selectedCustomerId]);

  const handleDeleteProfile = useCallback(async () => {
    if (!selectedCustomerId) return;
    const confirmed = window.confirm('确认回到系统整理稿吗？系统会立刻重新生成一份最新资料汇总。');
    if (!confirmed) return;
    try {
      await deleteCustomerProfileMarkdown(selectedCustomerId);
      await loadProfile(selectedCustomerId);
    } catch (err) {
      setError(err instanceof Error ? err.message : '恢复系统整理稿失败');
    }
  }, [loadProfile, selectedCustomerId]);

  const handleDeleteCustomer = useCallback(async () => {
    if (!selectedCustomerId || !selectedCustomer) return;
    const confirmed = window.confirm(`确认删除客户“${selectedCustomer.name}”及其全部相关数据吗？`);
    if (!confirmed) return;
    try {
      await deleteCustomer(selectedCustomerId);
      const nextCustomers = customers.filter((item) => item.record_id !== selectedCustomerId);
      setCustomers(nextCustomers);
      setSelectedCustomerId(nextCustomers[0]?.record_id ?? null);
      setProfile(null);
      setDraft('');
      if (!nextCustomers.length) {
        setCurrentCustomer(null, null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '删除客户失败');
    }
  }, [customers, selectedCustomer, selectedCustomerId, setCurrentCustomer]);

  const handlePreviewDocument = useCallback(async (document: CustomerDocumentListItem) => {
    if (!document.original_available) {
      setError('该资料未保存原件，仅保留提取结果和资料汇总。');
      return;
    }
    try {
      setError(null);
      await previewDocumentOriginal(document.doc_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : '查看原件失败');
    }
  }, []);

  const handleDownloadDocument = useCallback(async (document: CustomerDocumentListItem) => {
    if (!document.original_available) {
      setError('该资料未保存原件，仅保留提取结果和资料汇总。');
      return;
    }
    try {
      setError(null);
      await downloadDocumentOriginal(document.doc_id);
    } catch (err) {
      setError(err instanceof Error ? err.message : '下载原件失败');
    }
  }, []);

  const handleScrollToDocument = useCallback((document: CustomerDocumentListItem) => {
    setDocumentFilter('all');
    setFocusedConflictDocId(document.doc_id);
    const groupKey = getDocumentGroupKey(document.file_type);
    setCollapsedDocumentGroups((current) => ({
      ...current,
      [groupKey]: false,
    }));

    window.setTimeout(() => {
      const target = window.document.querySelector<HTMLElement>(`[data-doc-id="${document.doc_id}"]`);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, 280);
  }, []);

  const handleFocusConflictDocuments = useCallback((result: FieldConsistencyResult) => {
    const docIds = Array.from(new Set(
      result.comparedSources
        .map((source) => source.document?.doc_id || '')
        .filter(Boolean),
    ));
    if (docIds.length === 0) {
      return;
    }
    setDocumentFilter('all');
    setFocusedConflictDocIds(docIds);
    setFocusedConflictLabel(result.label);
    setCollapsedDocumentGroups((current) => {
      const next = { ...current };
      documents
        .filter((document) => docIds.includes(document.doc_id))
        .forEach((document) => {
          next[getDocumentGroupKey(document.file_type)] = false;
        });
      return next;
    });

    window.setTimeout(() => {
      const firstDocId = docIds[0];
      const target = window.document.querySelector<HTMLElement>(`[data-doc-id="${firstDocId}"]`);
      if (target) {
        target.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, 280);
  }, [documents]);

  const clearFocusedConflictDocuments = useCallback(() => {
    setFocusedConflictDocIds([]);
    setFocusedConflictLabel('');
  }, []);

  useEffect(() => {
    if (!focusedConflictDocId) {
      return;
    }
    const timer = window.setTimeout(() => {
      setFocusedConflictDocId('');
    }, 3200);
    return () => window.clearTimeout(timer);
  }, [focusedConflictDocId]);

  const handleRecommendedAction = useCallback((action: ActionType) => {
    if (!selectedCustomerId) {
      setError('未识别当前客户，暂时无法跳转。');
      return;
    }

    const targetPage = action === 'generate_application' ? 'application' : action === 'scheme_match' ? 'scheme' : 'upload';
    const targetPath = buildActionPath(action, selectedCustomerId, readinessSummary.missingLabels);
    window.history.pushState({}, '', targetPath);
    const navigated = clickExistingNavigation(targetPage);
    if (!navigated) {
      setError('页面跳转入口暂不可用，请从左侧导航进入对应页面。');
    }
  }, [readinessSummary.missingLabels, selectedCustomerId]);

  const toggleDocumentGroup = useCallback((groupKey: string) => {
    setCollapsedDocumentGroups((current) => ({
      ...current,
      [groupKey]: !current[groupKey],
    }));
  }, []);

  const expandAllDocumentGroups = useCallback(() => {
    setCollapsedDocumentGroups((current) => {
      const next = { ...current };
      groupedDocuments.forEach((group) => {
        next[group.key] = false;
      });
      return next;
    });
  }, [groupedDocuments]);

  const collapseAllDocumentGroups = useCallback(() => {
    setCollapsedDocumentGroups((current) => {
      const next = { ...current };
      groupedDocuments.forEach((group) => {
        next[group.key] = true;
      });
      return next;
    });
  }, [groupedDocuments]);

  return (
    <div className="flex h-full bg-slate-50">
      <aside className="w-72 border-r border-slate-200 bg-white">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-4">
          <div className="flex items-center gap-2">
            {onBack && (
              <button
                type="button"
                onClick={onBack}
                className="rounded-lg p-1.5 transition-colors hover:bg-slate-100"
                aria-label="返回"
              >
                <ArrowLeft className="h-4 w-4 text-slate-500" />
              </button>
            )}
            <div>
              <h1 className="text-sm font-semibold text-slate-800">资料汇总</h1>
              <p className="text-xs text-slate-400">客户资料整理与维护</p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => {
              void loadCustomers();
            }}
            className="rounded-lg border border-slate-200 p-2 text-slate-500 transition-colors hover:bg-slate-50"
            aria-label="刷新"
          >
            <RefreshCw className="h-4 w-4" />
          </button>
        </div>

        <div className="p-3">
          <div className="mb-3">
            <input
              type="text"
              value={customerSearch}
              onChange={(e) => setCustomerSearch(e.target.value)}
              placeholder="搜索客户名称"
              className="w-full rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700 outline-none transition-colors focus:border-blue-300 focus:bg-white"
            />
          </div>
          {loadingCustomers ? (
            <div className="py-8 text-center text-sm text-slate-400">加载客户中...</div>
          ) : customers.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
              暂无客户
            </div>
          ) : filteredCustomers.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
              未找到匹配客户
            </div>
          ) : (
            <div className="space-y-2">
              {filteredCustomers.map((customer) => {
                const active = customer.record_id === selectedCustomerId;
                return (
                  <button
                    key={customer.record_id}
                    type="button"
                    onClick={() => setSelectedCustomerId(customer.record_id)}
                    className={`relative w-full rounded-2xl border px-3 py-3 text-left transition-all ${
                      active
                        ? 'border-blue-300 bg-gradient-to-r from-blue-50 to-white shadow-sm shadow-blue-100'
                        : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'
                    }`}
                  >
                    {active && <span className="absolute inset-y-3 left-0 w-1 rounded-r-full bg-blue-500" />}
                    <div className="text-sm font-medium text-slate-800">{customer.name || customer.record_id}</div>
                    <div className="mt-2 text-xs text-slate-500">最近上传：{customer.upload_time || '未记录'}</div>
                    <div className="mt-2 text-xs text-slate-400">
                      {customer.customer_type === 'personal' ? '个人' : '企业'}
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </aside>

      <main className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">
              {selectedCustomer?.name || profile?.customer_name || '请选择客户'}
            </h2>
            {profile && (
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <span className="inline-flex items-center rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-600">
                  版本 {profileVersionLabel}
                </span>
                <span
                  className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${profileStatusClassName}`}
                >
                  {profileStatusLabel}
                </span>
                <span className="text-xs text-slate-400">最近更新：{formatProfileDateTime(profile?.updated_at)}</span>
              </div>
            )}
            {profile && <div className="mt-2 text-xs text-slate-400">{profileHintText}</div>}
          </div>

          <div className="flex items-center gap-2">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-1">
              <button
                type="button"
                onClick={() => setMode('edit')}
                className={`rounded-lg px-3 py-1.5 text-sm ${
                  mode === 'edit' ? 'bg-white text-slate-800 shadow-sm' : 'text-slate-500'
                }`}
              >
                <span className="inline-flex items-center gap-1">
                  <Pencil className="h-3.5 w-3.5" />
                  编辑
                </span>
              </button>
              <button
                type="button"
                onClick={() => setMode('preview')}
                className={`rounded-lg px-3 py-1.5 text-sm ${
                  mode === 'preview' ? 'bg-white text-slate-800 shadow-sm' : 'text-slate-500'
                }`}
              >
                <span className="inline-flex items-center gap-1">
                  <Eye className="h-3.5 w-3.5" />
                  预览
                </span>
              </button>
            </div>

            <button
              type="button"
              onClick={() => selectedCustomerId && void loadProfile(selectedCustomerId)}
              disabled={!selectedCustomerId || loadingProfile}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              刷新
            </button>
            <button
              type="button"
              onClick={() => void handleSave()}
              disabled={!selectedCustomerId || saving || !isDirty}
              className="rounded-lg bg-blue-600 px-3 py-2 text-sm font-medium text-white shadow-sm shadow-blue-200 transition-colors hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="inline-flex items-center gap-1">
                <Save className="h-3.5 w-3.5" />
                {saving ? '保存中...' : '保存'}
              </span>
            </button>
            <button
              type="button"
              onClick={() => void handleDeleteProfile()}
              disabled={!selectedCustomerId}
              className="rounded-lg border border-amber-200 bg-amber-50/70 px-3 py-2 text-sm text-amber-700 transition-colors hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              回到系统整理稿
            </button>
            <button
              type="button"
              onClick={() => void handleDeleteCustomer()}
              disabled={!selectedCustomerId}
              className="rounded-lg border border-red-200 bg-white px-3 py-2 text-sm text-red-600 transition-colors hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="inline-flex items-center gap-1">
                <Trash2 className="h-3.5 w-3.5" />
                删除客户
              </span>
            </button>
          </div>
        </div>

        <div className="border-b border-slate-200 bg-slate-50/80 px-6 py-3">
          <div className="flex flex-wrap items-center gap-2 text-sm text-slate-600">
            <span className="rounded-full bg-white px-2.5 py-1 text-xs font-medium text-slate-600 shadow-sm">
              本页说明
            </span>
            <span>系统会先整理当前客户的核心资料，你也可以继续补充、修订，并保存为当前使用版本。</span>
          </div>
        </div>

        {error && (
          <div className="border-b border-red-100 bg-red-50 px-6 py-3 text-sm text-red-600">{error}</div>
        )}

        {saveSuccess && !error && (
          <div className="border-b border-emerald-100 bg-emerald-50 px-6 py-3 text-sm text-emerald-700">
            已为当前客户保存最新资料整理内容，资料问答会优先读取这份版本。
          </div>
        )}

        {isDirty && !saving && !error && (
          <div className="border-b border-amber-100 bg-amber-50 px-6 py-3 text-sm text-amber-700">
            当前有未保存修改。保存后，资料问答和风险评估会优先读取这份最新内容。
          </div>
        )}

        <div className="border-b border-slate-200 bg-white px-6 py-4">
          <ProcessFeedbackCard
            tone={profileFeedback.tone}
            title={profileFeedback.title}
            description={profileFeedback.description}
            persistenceHint={profileFeedback.persistenceHint}
            nextStep={profileFeedback.nextStep}
          />
        </div>

        {selectedCustomerId ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            {(() => {
              const overallMeta = getReadinessStatusMeta(readinessSummary.overallStatus);
              return (
                <div className={`rounded-3xl border p-5 ${overallMeta.cardClassName}`}>
                  <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <h3 className="text-base font-semibold text-slate-900">整体结论与下一步建议</h3>
                        <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${overallMeta.className}`}>
                          {overallMeta.label}
                        </span>
                      </div>
                      <p className="mt-2 text-sm font-medium text-slate-800">{readinessSummary.overallTitle}</p>
                      <p className="mt-1 text-sm leading-6 text-slate-600">{readinessSummary.overallDescription}</p>
                      {readinessSummary.missingLabels.length > 0 ? (
                        <div className="mt-3 rounded-2xl border border-white/70 bg-white/70 px-3 py-2 text-sm text-slate-700">
                          优先补充：{readinessSummary.missingLabels.join('、')}
                        </div>
                      ) : null}
                      {conflictingConsistencyResults.length > 0 ? (
                        <div className="mt-3 rounded-2xl border border-orange-200 bg-orange-50 px-3 py-3 text-sm text-orange-800">
                          <div className="font-semibold">
                            当前存在 {conflictingConsistencyResults.length} 个关键字段冲突
                          </div>
                          <div className="mt-1">
                            建议先核对：{conflictingConsistencyResults.map((item) => item.label).join('、')}，再继续生成申请表或风控报告。
                          </div>
                        </div>
                      ) : null}
                    </div>

                    <div className="flex shrink-0 flex-wrap gap-2">
                      {readinessSummary.actions.map((action) => (
                        <button
                          key={action}
                          type="button"
                          onClick={() => handleRecommendedAction(action)}
                          className={`rounded-xl px-4 py-2 text-sm font-medium shadow-sm transition-colors ${
                            action === 'upload_missing'
                              ? 'border border-amber-200 bg-white text-amber-700 hover:bg-amber-50'
                              : 'border border-blue-200 bg-blue-600 text-white hover:bg-blue-700'
                          }`}
                        >
                          {getActionLabel(action)}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="mt-5 grid gap-3 xl:grid-cols-3">
                    {readinessSummary.items.map((item) => {
                      const itemMeta = getReadinessStatusMeta(item.status);
                      return (
                        <article key={item.key} className="rounded-2xl border border-white/80 bg-white/80 p-4">
                          <div className="flex items-start justify-between gap-3">
                            <h4 className="text-sm font-semibold text-slate-800">{item.title}</h4>
                            <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${itemMeta.className}`}>
                              {itemMeta.label}
                            </span>
                          </div>
                          <p className="mt-2 text-sm leading-6 text-slate-600">{item.message}</p>
                          {item.suggestion ? (
                            <p className="mt-2 text-xs leading-5 text-slate-500">{item.suggestion}</p>
                          ) : null}
                        </article>
                      );
                    })}
                  </div>
                </div>
              );
            })()}
          </section>
        ) : null}

        {selectedCustomerId && companyArticlesInsight ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <h3 className="text-base font-semibold text-slate-800">公司章程重点信息</h3>
                  <p className="mt-1 text-sm text-slate-500">
                    这里聚合展示公司章程中和股权结构、融资审批、重大事项决策直接相关的内容，方便快速核对。
                  </p>
                </div>
                {companyArticlesInsight.document ? (
                  <div className="flex shrink-0 flex-wrap gap-2">
                    {companyArticlesInsight.document.original_available ? (
                      <>
                        <button
                          type="button"
                          onClick={() => void handlePreviewDocument(companyArticlesInsight.document as CustomerDocumentListItem)}
                          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                        >
                          查看原件
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleDownloadDocument(companyArticlesInsight.document as CustomerDocumentListItem)}
                          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                        >
                          下载原件
                        </button>
                      </>
                    ) : (
                      <span className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-500">
                        仅保留提取结果
                      </span>
                    )}
                  </div>
                ) : null}
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                <article className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-slate-800">股权结构</h4>
                      <p className="mt-1 text-xs leading-5 text-slate-500">
                        优先展示注册资本、股东数量、股权结构摘要和逐位股东明细。
                      </p>
                    </div>
                    <span className="rounded-full border border-violet-200 bg-violet-50 px-2.5 py-1 text-xs font-medium text-violet-700">
                      股东 {companyArticlesInsight.shareholderCount || companyArticlesShareholderViews.length || '暂无'} 位
                    </span>
                  </div>

                  <div className="mt-4 space-y-3 text-sm">
                    <div>
                      <div className="text-xs font-medium text-slate-500">公司名称</div>
                      <div className="mt-1 text-slate-700">{companyArticlesInsight.companyName || '暂无'}</div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">注册资本</div>
                      <div className="mt-1 text-slate-700">{companyArticlesInsight.registeredCapital || '暂无'}</div>
                    </div>
                    {companyArticlesRoleItems.length > 0 ? (
                      <div className="grid gap-3 md:grid-cols-2">
                        {companyArticlesRoleItems.map((item) => (
                          <div key={item.label}>
                            <div className="text-xs font-medium text-slate-500">{item.label}</div>
                            <div className="mt-1 text-slate-700">{item.value}</div>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="rounded-xl border border-slate-200 bg-white px-3 py-3 text-sm text-slate-600">
                        章程未明确列出具体任职姓名。
                      </div>
                    )}
                    {companyArticlesInsight.managementRolesSummary && companyArticlesInsight.managementRolesSummary !== '暂无' ? (
                      <div>
                        <div className="text-xs font-medium text-slate-500">任职信息摘要</div>
                        <div className="mt-1 text-slate-700">{companyArticlesInsight.managementRolesSummary}</div>
                      </div>
                    ) : null}
                    {companyArticlesInsight.address || companyArticlesInsight.managementStructure ? (
                      <div className="grid gap-3 md:grid-cols-2">
                        {companyArticlesInsight.address ? (
                          <div>
                            <div className="text-xs font-medium text-slate-500">地址</div>
                            <div className="mt-1 text-slate-700">{companyArticlesInsight.address}</div>
                          </div>
                        ) : null}
                        {companyArticlesInsight.managementStructure ? (
                          <div>
                            <div className="text-xs font-medium text-slate-500">治理结构</div>
                            <div className="mt-1 text-slate-700">{companyArticlesInsight.managementStructure}</div>
                          </div>
                        ) : null}
                      </div>
                    ) : null}
                    {companyArticlesInsight.summary ? (
                      <div className="rounded-xl border border-slate-200 bg-white px-3 py-3">
                        <div className="text-xs font-medium text-slate-500">章程摘要</div>
                        <div className="mt-1 text-sm leading-6 text-slate-700">{companyArticlesInsight.summary}</div>
                      </div>
                    ) : null}
                    <div className="rounded-xl border border-amber-200 bg-amber-50/80 px-3 py-3">
                      <div className="text-xs font-medium text-amber-700">任职原文线索（调试）</div>
                      {companyArticlesInsight.managementRoleEvidenceLines.length > 0 ? (
                        <div className="mt-2 space-y-1 text-xs leading-5 text-slate-700">
                          {companyArticlesInsight.managementRoleEvidenceLines.map((line, index) => (
                            <div key={`${line}-${index}`} className="rounded-lg bg-white/80 px-2 py-1">
                              {line}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="mt-2 text-xs leading-5 text-slate-600">
                          当前未拿到包含法定代表人 / 执行董事 / 经理 / 监事的明确 OCR 原文线索。
                        </div>
                      )}
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">股权结构摘要</div>
                      <div className="mt-1 text-slate-700">
                        {companyArticlesInsight.equityStructureSummary || '暂未提取到明确股权结构摘要'}
                      </div>
                    </div>
                    {companyArticlesControlSummary ? (
                      <div className="rounded-xl border border-violet-200 bg-violet-50/70 px-3 py-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="rounded-full border border-violet-200 bg-white px-2 py-0.5 text-xs font-medium text-violet-700">
                            {companyArticlesControlSummary.label}
                          </span>
                        </div>
                        <div className="mt-1 text-xs leading-5 text-slate-600">
                          {companyArticlesControlSummary.description}
                        </div>
                      </div>
                    ) : null}
                    <div>
                      <div className="text-xs font-medium text-slate-500">股东列表</div>
                      {companyArticlesShareholderViews.length > 0 ? (
                        <div className="mt-2 space-y-2">
                          {companyArticlesShareholderViews.map((shareholder, index) => {
                            const parts = [shareholder.contribution, shareholder.method, shareholder.date, shareholder.ratio]
                              .filter((item) => item && item !== '暂无');
                            return (
                              <div
                                key={`${shareholder.name}-${index}`}
                                className={`rounded-xl border bg-white px-3 py-3 ${
                                  shareholder.isPrimary
                                    ? 'border-violet-200 shadow-sm shadow-violet-100/80'
                                    : 'border-slate-200'
                                }`}
                              >
                                <div className="flex flex-wrap items-center gap-2">
                                  <div className="text-sm font-medium text-slate-800">
                                    {shareholder.name || `股东 ${index + 1}`}
                                  </div>
                                  {shareholder.isPrimary ? (
                                    <span className="rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-xs font-medium text-violet-700">
                                      主要股东
                                    </span>
                                  ) : null}
                                  {shareholder.ratio && shareholder.ratio !== '暂无' ? (
                                    <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs text-slate-600">
                                      {shareholder.ratio}
                                    </span>
                                  ) : null}
                                </div>
                                <div className="mt-1 text-xs leading-5 text-slate-500">
                                  {parts.length > 0 ? parts.join('｜') : '暂无更多股东明细'}
                                </div>
                                <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                                  <span>{getCompanyArticlesSourceLabel(companyArticlesInsight.document)}</span>
                                  {companyArticlesInsight.document?.original_available ? (
                                    <button
                                      type="button"
                                      onClick={() => void handlePreviewDocument(companyArticlesInsight.document as CustomerDocumentListItem)}
                                      className="rounded-lg border border-slate-200 bg-white px-2.5 py-1 text-xs text-slate-600 transition-colors hover:bg-slate-50"
                                    >
                                      查看来源
                                    </button>
                                  ) : null}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      ) : (
                        <div className="mt-1 text-slate-700">暂无</div>
                      )}
                    </div>
                  </div>
                </article>

                <article className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-slate-800">融资与重大事项规则</h4>
                      <p className="mt-1 text-xs leading-5 text-slate-500">
                        重点查看融资、贷款、担保、章程修改等事项的审批规则和门槛。
                      </p>
                    </div>
                    <span className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-xs font-medium text-amber-700">
                      门槛 {companyArticlesInsight.financingApprovalThreshold || '待核对'}
                    </span>
                  </div>

                  <div className="mt-4 space-y-3 text-sm">
                    <div>
                      <div className="text-xs font-medium text-slate-500">融资/贷款审批规则</div>
                      <div className="mt-1 text-slate-700">
                        {companyArticlesInsight.financingApprovalRule || '暂未提取到明确审批规则'}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">融资表决门槛</div>
                      <div className="mt-1 text-slate-700">{companyArticlesInsight.financingApprovalThreshold || '暂无'}</div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">重大事项规则明细</div>
                      {companyArticlesRuleGroups.length > 0 ? (
                        <div className="mt-2 space-y-3">
                          {companyArticlesRuleGroups.map((group, groupIndex) => (
                            <div key={`${group.topic}-${groupIndex}`} className="rounded-xl border border-slate-200 bg-white px-3 py-3">
                              <div className="flex flex-wrap items-center gap-2">
                                <span className={`rounded-full border px-2 py-0.5 text-xs font-medium ${getRuleTopicBadgeClass(group.topic)}`}>
                                  {group.topic || `事项组 ${groupIndex + 1}`}
                                </span>
                                <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs text-slate-500">
                                  {group.items.length} 条
                                </span>
                              </div>
                              <div className="mt-3 space-y-2">
                                {group.items.map((item, index) => (
                                  <div key={`${group.topic}-${index}`} className="rounded-lg border border-slate-100 bg-slate-50/70 px-3 py-2">
                                  <div className="flex flex-wrap items-center gap-2">
                                    {item.threshold && item.threshold !== '暂无' ? (
                                      <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-600">
                                        门槛：{item.threshold}
                                      </span>
                                    ) : null}
                                  </div>
                                  <div className="mt-1 text-xs leading-5 text-slate-500">{item.rule || '暂无规则原文'}</div>
                                  <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                                    <span>{getCompanyArticlesSourceLabel(companyArticlesInsight.document)}</span>
                                    {companyArticlesInsight.document?.original_available ? (
                                      <button
                                        type="button"
                                        onClick={() => void handlePreviewDocument(companyArticlesInsight.document as CustomerDocumentListItem)}
                                        className="rounded-lg border border-slate-200 bg-white px-2.5 py-1 text-xs text-slate-600 transition-colors hover:bg-slate-50"
                                      >
                                        查看来源
                                      </button>
                                    ) : null}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                        </div>
                      ) : (
                        <div className="mt-1 text-slate-700">暂无</div>
                      )}
                    </div>
                  </div>
                </article>
              </div>
            </div>
          </section>
        ) : null}

        {selectedCustomerId ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div>
                <h3 className="text-base font-semibold text-slate-800">资料完整度</h3>
                <p className="mt-1 text-sm text-slate-500">
                  先看当前客户的关键资料是否满足后续资料汇总、申请表、方案匹配和风控判断的最低要求。
                </p>
              </div>

              <div className="grid gap-3 xl:grid-cols-2 2xl:grid-cols-4">
                {completenessCards.map((card) => {
                  const statusMeta = getCompletenessStatusMeta(card.status);
                  return (
                    <article
                      key={card.key}
                      className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <h4 className="text-sm font-semibold text-slate-800">{card.title}</h4>
                          <p className="mt-1 text-xs leading-5 text-slate-500">{card.description}</p>
                        </div>
                        <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${statusMeta.className}`}>
                          {statusMeta.label}
                        </span>
                      </div>

                      <div className="mt-4 space-y-3 text-sm">
                        <div>
                          <div className="text-xs font-medium text-slate-500">已有资料</div>
                          <div className="mt-1 text-slate-700">
                            {card.existingLabels.length > 0 ? card.existingLabels.join('、') : '暂无'}
                          </div>
                        </div>

                        <div>
                          <div className="text-xs font-medium text-slate-500">
                            {card.key === 'asset' ? '可补充资料' : '缺失关键资料'}
                          </div>
                          <div className="mt-1 text-slate-700">
                            {card.missingRequiredLabels.length > 0
                              ? card.missingRequiredLabels.join('、')
                              : card.key === 'asset'
                                ? '暂无资产类资料'
                                : '无'}
                          </div>
                        </div>

                        {card.missingOptionalLabels.length > 0 ? (
                          <div>
                            <div className="text-xs font-medium text-slate-500">可继续补充</div>
                            <div className="mt-1 text-slate-700">{card.missingOptionalLabels.join('、')}</div>
                          </div>
                        ) : null}
                      </div>
                    </article>
                  );
                })}
              </div>
            </div>
          </section>
        ) : null}

        {selectedCustomerId && hukouInsight ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <h3 className="text-base font-semibold text-slate-800">户口本重点信息</h3>
                  <p className="mt-1 text-sm text-slate-500">
                    这里直接展示户口本首页和家庭成员的结构化结果，优先便于核对户主、地址和家庭成员信息。
                  </p>
                </div>
                {hukouInsight.document ? (
                  <div className="flex shrink-0 flex-wrap gap-2">
                    {hukouInsight.document.original_available ? (
                      <>
                        <button
                          type="button"
                          onClick={() => void handlePreviewDocument(hukouInsight.document as CustomerDocumentListItem)}
                          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                        >
                          查看原件
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleDownloadDocument(hukouInsight.document as CustomerDocumentListItem)}
                          className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                        >
                          下载原件
                        </button>
                      </>
                    ) : (
                      <span className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-500">
                        仅保留提取结果
                      </span>
                    )}
                  </div>
                ) : null}
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                <article className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-slate-800">户口本首页</h4>
                      <p className="mt-1 text-xs leading-5 text-slate-500">
                        优先核对户主姓名、户号、户别、户籍地址和登记机关。
                      </p>
                    </div>
                    <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-xs font-medium text-emerald-700">
                      {hukouInsight.completenessNote || '已提取'}
                    </span>
                  </div>

                  <div className="mt-4 grid gap-3 md:grid-cols-2 text-sm">
                    <div>
                      <div className="text-xs font-medium text-slate-500">户主姓名</div>
                      <div className="mt-1 text-slate-700">{hukouInsight.householdHeadName || '暂无'}</div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">户号</div>
                      <div className="mt-1 text-slate-700">{hukouInsight.householdNumber || '暂无'}</div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">户别</div>
                      <div className="mt-1 text-slate-700">{hukouInsight.householdType || '暂无'}</div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-slate-500">登记机关</div>
                      <div className="mt-1 text-slate-700">{hukouInsight.registrationAuthority || '暂无'}</div>
                    </div>
                    <div className="md:col-span-2">
                      <div className="text-xs font-medium text-slate-500">户籍地址</div>
                      <div className="mt-1 text-slate-700 break-words">{hukouInsight.householdAddress || '暂无'}</div>
                    </div>
                  </div>
                </article>

                <article className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-slate-800">家庭成员</h4>
                      <p className="mt-1 text-xs leading-5 text-slate-500">
                        优先展示姓名、关系、性别、民族、出生日期、身份证号码和婚姻状况。
                      </p>
                    </div>
                    <span className="rounded-full border border-violet-200 bg-violet-50 px-2.5 py-1 text-xs font-medium text-violet-700">
                      成员 {hukouInsight.members.length} 人
                    </span>
                  </div>

                  {hukouInsight.members.length > 0 ? (
                    <div className="mt-4 overflow-x-auto">
                      <table className="min-w-full border-separate border-spacing-0 overflow-hidden rounded-xl border border-slate-200 bg-white text-sm">
                        <thead className="bg-slate-100/90 text-slate-600">
                          <tr>
                            {['姓名', '与户主关系', '性别', '民族', '出生日期', '身份证号码', '婚姻状况'].map((label) => (
                              <th key={label} className="border-b border-slate-200 px-3 py-2 text-left text-xs font-medium">
                                {label}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {hukouInsight.members.map((member, index) => (
                            <tr key={`${stringifyExtractionValue(member.id_number || member['身份证号码'] || member['身份证号']) || stringifyExtractionValue(member.name || member['姓名'])}-${index}`} className="odd:bg-white even:bg-slate-50/50">
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.name || member['姓名']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.relationship_to_head || member['与户主关系'] || member['户主或与户主关系']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.gender || member['性别']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.ethnicity || member['民族']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.birth_date || member['出生日期']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 font-mono text-slate-700">{stringifyExtractionValue(member.id_number || member['身份证号码'] || member['身份证号']) || '—'}</td>
                              <td className="border-b border-slate-100 px-3 py-2 text-slate-700">{stringifyExtractionValue(member.marital_status || member['婚姻状况']) || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-white px-4 py-6 text-sm text-slate-500">
                      暂未识别到家庭成员信息。
                    </div>
                  )}
                </article>
              </div>
            </div>
          </section>
        ) : null}

        {selectedCustomerId ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div>
                <h3 className="text-base font-semibold text-slate-800">关键字段来源回查</h3>
                <p className="mt-1 text-sm text-slate-500">
                  先按字段与资料类型规则追踪来源，帮助快速判断字段来自哪份资料，以及是否可以查看原件。
                </p>
              </div>

              {fieldSourceSummaries.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
                  暂未识别到可回查来源的关键字段，请先上传营业执照、开户许可证、银行对账单或身份证等资料。
                </div>
              ) : (
                <div className="grid gap-3 xl:grid-cols-2">
                  {fieldSourceSummaries.map((field) => (
                    <article key={field.fieldKey} className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                        <div className="min-w-0">
                          <h4 className="text-sm font-semibold text-slate-800">{field.label}</h4>
                          <div className="mt-1 break-words text-sm text-slate-700">
                            字段值：{field.value || '资料汇总中暂未识别到明确字段值'}
                          </div>
                        </div>
                        <span className="shrink-0 rounded-full border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-medium text-blue-700">
                          来源 {field.sources.length} 份
                        </span>
                      </div>

                      {field.sources.length > 0 ? (
                        <div className="mt-4 space-y-2">
                          {field.sources.map((document) => (
                            <div
                              key={`${field.fieldKey}-${document.doc_id}`}
                              className="rounded-xl border border-slate-200 bg-white px-3 py-3"
                            >
                              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                                <div className="min-w-0 flex-1">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <span className="truncate text-sm font-medium text-slate-800">
                                      {document.file_name || '未命名文件'}
                                    </span>
                                    <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs text-slate-500">
                                      {getDocumentTypeDisplayLabel(document, extractionGroups)}
                                    </span>
                                    <span
                                      className={`rounded-full border px-2 py-0.5 text-xs font-medium ${
                                        document.original_available
                                          ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                          : 'border-sky-200 bg-sky-50 text-sky-700'
                                      }`}
                                    >
                                      {document.original_available ? '可查看原件' : '仅保留提取结果'}
                                    </span>
                                  </div>
                                  <div className="mt-1 text-xs text-slate-500">
                                    上传时间：{formatProfileDateTime(document.upload_time)}
                                  </div>
                                </div>

                                <div className="flex shrink-0 flex-wrap gap-2">
                                  {document.original_available ? (
                                    <>
                                      <button
                                        type="button"
                                        onClick={() => void handlePreviewDocument(document)}
                                        className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                      >
                                        查看原件
                                      </button>
                                      <button
                                        type="button"
                                        onClick={() => void handleDownloadDocument(document)}
                                        className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                      >
                                        下载原件
                                      </button>
                                    </>
                                  ) : (
                                    <span className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-500">
                                      不提供原件按钮
                                    </span>
                                  )}
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="mt-4 rounded-xl border border-dashed border-slate-200 bg-white px-3 py-3 text-sm text-slate-500">
                          当前客户暂无可匹配的来源文档，建议补充对应资料后再回查。
                        </div>
                      )}
                    </article>
                  ))}
                </div>
              )}
            </div>
          </section>
        ) : null}

        {selectedCustomerId ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div>
                <h3 className="text-base font-semibold text-slate-800">字段冲突与一致性提示</h3>
                <p className="mt-1 text-sm text-slate-500">
                  基于当前客户已保存的结构化提取结果，对关键字段做多来源比对；建议先核验冲突字段后再生成申请表或风控报告。
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3">
                  <div className="text-xs font-medium text-emerald-700">一致字段</div>
                  <div className="mt-1 text-2xl font-semibold text-emerald-800">{consistentConsistencyResults.length}</div>
                </div>
                <div className="rounded-2xl border border-orange-200 bg-orange-50 px-4 py-3">
                  <div className="text-xs font-medium text-orange-700">存在差异</div>
                  <div className="mt-1 text-2xl font-semibold text-orange-800">{conflictingConsistencyResults.length}</div>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                  <div className="text-xs font-medium text-slate-600">信息不足</div>
                  <div className="mt-1 text-2xl font-semibold text-slate-700">{insufficientConsistencyResults.length}</div>
                </div>
              </div>

              <div
                className={`rounded-2xl border px-4 py-3 text-sm ${
                  conflictingConsistencyResults.length > 0
                    ? 'border-orange-200 bg-orange-50 text-orange-800'
                    : 'border-emerald-200 bg-emerald-50 text-emerald-800'
                }`}
              >
                {conflictingConsistencyResults.length > 0 ? (
                  <>
                    <div className="font-semibold">
                      当前发现 {conflictingConsistencyResults.length} 个关键字段存在差异
                    </div>
                    <div className="mt-1">
                      建议优先核对：{conflictingConsistencyResults.map((item) => item.label).join('、')}
                    </div>
                    <div className="mt-1">
                      建议先核对营业执照、公司章程、开户许可证等原件，确认字段后再继续生成申请表或风控报告。
                    </div>
                  </>
                ) : (
                  <div className="font-semibold">当前未发现关键字段冲突</div>
                )}
              </div>

              {fieldConsistencyResults.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
                  暂无可用于一致性检查的结构化字段。上传并提取关键资料后，这里会显示字段是否一致。
                </div>
              ) : (
                <div className="space-y-4">
                  {conflictingConsistencyResults.length > 0 ? (
                    <div className="grid gap-3 xl:grid-cols-2">
                      {conflictingConsistencyResults.map((result) => {
                        const statusMeta = getFieldConsistencyStatusMeta(result.status);
                        const prioritySource = getPrioritySource(result.fieldKey, result.comparedSources);
                        const actionSuggestion = getActionSuggestion(result.fieldKey);
                        const impactTags = getFieldImpactTags(result.fieldKey);
                        return (
                          <article
                            key={result.fieldKey}
                            className={`rounded-2xl border p-4 ${statusMeta.cardClassName}`}
                          >
                            <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                              <div>
                                <h4 className="text-sm font-semibold text-slate-800">{result.label}</h4>
                                <p className="mt-1 text-xs text-slate-500">
                                  参与比对来源：{result.comparedSources.length} 个
                                </p>
                              </div>
                              <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${statusMeta.className}`}>
                                {statusMeta.label}
                              </span>
                            </div>

                            {impactTags.length > 0 ? (
                              <div className="mt-3 flex flex-wrap gap-2">
                                {impactTags.map((tag) => (
                                  <span
                                    key={`${result.fieldKey}-${tag}`}
                                    className="rounded-full border border-orange-200 bg-orange-100 px-2.5 py-1 text-xs font-medium text-orange-700"
                                  >
                                    {tag}
                                  </span>
                                ))}
                              </div>
                            ) : null}

                            <div className="mt-4 rounded-xl border border-orange-200 bg-white/90 px-3 py-3 text-sm text-orange-800">
                              <div>
                                <span className="font-semibold">建议优先：</span>
                                {prioritySource?.documentTypeName || '请人工确认更可信来源'}
                              </div>
                              <div className="mt-1">
                                <span className="font-semibold">操作建议：</span>
                                {actionSuggestion}
                              </div>
                              <div className="mt-3">
                                <button
                                  type="button"
                                  onClick={() => handleFocusConflictDocuments(result)}
                                  className="rounded-lg border border-orange-200 bg-orange-50 px-3 py-1.5 text-xs font-medium text-orange-700 transition-colors hover:bg-orange-100"
                                >
                                  仅看冲突来源文档
                                </button>
                              </div>
                            </div>

                            <div className="mt-4 space-y-2">
                              {result.comparedSources.map((source) => (
                                <div
                                  key={`${result.fieldKey}-${source.extractionId}-${source.documentType}-${source.rawValue}`}
                                  className="rounded-xl border border-white/80 bg-white/90 px-3 py-3"
                                >
                                  <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                                    <div className="min-w-0 flex-1">
                                      <div className="flex flex-wrap items-center gap-2">
                                        <span className="text-sm font-medium text-slate-800">
                                          {source.documentTypeName}
                                        </span>
                                        <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs text-slate-500">
                                          {source.fileName}
                                        </span>
                                        <span
                                          className={`rounded-full border px-2 py-0.5 text-xs font-medium ${
                                            source.document?.original_available
                                              ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                              : 'border-sky-200 bg-sky-50 text-sky-700'
                                          }`}
                                        >
                                          {source.document?.original_available ? '可查看原件' : '仅保留提取结果'}
                                        </span>
                                      </div>
                                      <div className="mt-2 text-sm text-slate-700">
                                        <div className="font-medium text-slate-600">字段值</div>
                                        <div className="mt-1 space-y-1">
                                          {formatConsistencyValueForDisplay(result.fieldKey, source.rawValue).map((line, index) => (
                                            <div key={`${result.fieldKey}-${source.extractionId}-line-${index}`} className="break-words">
                                              {line}
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    </div>

                                      <div className="flex shrink-0 flex-wrap gap-2">
                                        {source.document?.original_available ? (
                                          <>
                                            <button
                                              type="button"
                                              onClick={() => handleScrollToDocument(source.document as CustomerDocumentListItem)}
                                              className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                            >
                                              定位来源文档
                                            </button>
                                            <button
                                              type="button"
                                              onClick={() => void handlePreviewDocument(source.document as CustomerDocumentListItem)}
                                              className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                            >
                                            查看原件
                                          </button>
                                          <button
                                            type="button"
                                            onClick={() => void handleDownloadDocument(source.document as CustomerDocumentListItem)}
                                            className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                          >
                                            下载原件
                                          </button>
                                          </>
                                        ) : (
                                          <>
                                            <button
                                              type="button"
                                              onClick={() => handleScrollToDocument(source.document as CustomerDocumentListItem)}
                                              className="rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs text-slate-700 transition-colors hover:bg-slate-50"
                                            >
                                              定位来源文档
                                            </button>
                                            <span className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs text-slate-500">
                                              仅保留提取结果
                                            </span>
                                          </>
                                        )}
                                      </div>
                                  </div>
                                </div>
                              ))}
                            </div>
                          </article>
                        );
                      })}
                    </div>
                  ) : null}

                  {insufficientConsistencyResults.length > 0 ? (
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                      <div className="flex items-center justify-between gap-3">
                        <div>
                          <h4 className="text-sm font-semibold text-slate-700">信息不足字段</h4>
                          <p className="mt-1 text-xs text-slate-500">
                            以下字段当前只有单一来源或暂无足够值可比对，不视为冲突。
                          </p>
                        </div>
                        <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs text-slate-600">
                          {insufficientConsistencyResults.length} 项
                        </span>
                      </div>
                      <div className="mt-3 grid gap-2 xl:grid-cols-2">
                        {insufficientConsistencyResults.map((result) => (
                          <div
                            key={result.fieldKey}
                            className="rounded-xl border border-slate-200 bg-white px-3 py-3 text-sm text-slate-600"
                          >
                            <div className="font-medium text-slate-700">{result.label}</div>
                            <div className="mt-1 text-xs text-slate-500">
                              {result.comparedSources.length === 1
                                ? `当前仅 ${result.comparedSources[0].documentTypeName} 提供了该字段，暂无其他来源可比对。`
                                : '当前没有足够的非空来源值，暂不做冲突判断。'}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : null}

                  {consistentConsistencyResults.length > 0 ? (
                    <div className="rounded-2xl border border-emerald-200 bg-emerald-50/60 p-4">
                      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                        <div>
                          <h4 className="text-sm font-semibold text-emerald-800">一致字段</h4>
                          <p className="mt-1 text-xs text-emerald-700">
                            多来源字段标准化后结果一致，默认折叠展示。
                          </p>
                        </div>
                        <button
                          type="button"
                          onClick={() => setShowConsistentFields((current) => !current)}
                          className="rounded-lg border border-emerald-200 bg-white px-3 py-1.5 text-xs font-medium text-emerald-700 transition-colors hover:bg-emerald-50"
                        >
                          {showConsistentFields ? '收起一致字段' : '查看一致字段'}
                        </button>
                      </div>

                      {showConsistentFields ? (
                        <div className="mt-3 grid gap-3 xl:grid-cols-2">
                          {consistentConsistencyResults.map((result) => {
                            const statusMeta = getFieldConsistencyStatusMeta(result.status);
                            return (
                              <article
                                key={result.fieldKey}
                                className={`rounded-2xl border p-4 ${statusMeta.cardClassName}`}
                              >
                                <div className="flex items-start justify-between gap-3">
                                  <div>
                                    <h4 className="text-sm font-semibold text-slate-800">{result.label}</h4>
                                    <p className="mt-1 text-xs text-slate-500">
                                      参与比对来源：{result.comparedSources.length} 个
                                    </p>
                                  </div>
                                  <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${statusMeta.className}`}>
                                    {statusMeta.label}
                                  </span>
                                </div>
                                <div className="mt-3 space-y-2">
                                  {result.comparedSources.map((source) => (
                                    <div
                                      key={`${result.fieldKey}-${source.extractionId}-${source.documentType}-${source.rawValue}`}
                                      className="rounded-xl border border-white/80 bg-white/90 px-3 py-3"
                                    >
                                      <div className="text-sm font-medium text-slate-800">{source.documentTypeName}</div>
                                      <div className="mt-1 space-y-1 text-sm text-slate-700">
                                        {formatConsistencyValueForDisplay(result.fieldKey, source.rawValue).map((line, index) => (
                                          <div key={`${result.fieldKey}-${source.extractionId}-consistent-${index}`} className="break-words">
                                            {line}
                                          </div>
                                        ))}
                                      </div>
                                      <div className="mt-1 text-xs text-slate-500">{source.fileName}</div>
                                    </div>
                                  ))}
                                </div>
                              </article>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              )}
            </div>
          </section>
        ) : null}

        {selectedCustomerId ? (
          <section className="border-b border-slate-200 bg-white px-6 py-5">
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <h3 className="text-base font-semibold text-slate-800">来源文档与原件状态</h3>
                  <p className="mt-1 text-sm text-slate-500">
                    这里会集中展示当前客户的来源文件、原件状态、最新版本标识，以及可用的原件查看操作。
                  </p>
                </div>
                <div className="flex flex-wrap items-center gap-2 text-xs">
                  <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1.5 text-slate-600">
                    全部 {documents.length} 份
                  </span>
                  <span className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-emerald-700">
                    可查看原件 {originalDocumentCount} 份
                  </span>
                  <span className="rounded-full border border-sky-200 bg-sky-50 px-3 py-1.5 text-sky-700">
                    仅保留提取结果 {extractionOnlyDocumentCount} 份
                  </span>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                {([
                  ['all', '全部'],
                  ['original', '可查看原件'],
                  ['extraction', '仅提取结果'],
                ] as const).map(([modeValue, label]) => (
                  <button
                    key={modeValue}
                    type="button"
                    onClick={() => setDocumentFilter(modeValue)}
                    className={`rounded-full border px-3 py-1.5 text-sm transition-colors ${
                      documentFilter === modeValue
                        ? 'border-blue-200 bg-blue-50 text-blue-700'
                        : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    {label}
                  </button>
                ))}
                {focusedConflictDocIds.length > 0 ? (
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="rounded-full border border-orange-200 bg-orange-50 px-3 py-1.5 text-sm text-orange-700">
                      当前仅显示与“{focusedConflictLabel || '冲突字段'}”相关的来源文档
                    </span>
                    <button
                      type="button"
                      onClick={clearFocusedConflictDocuments}
                      className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
                    >
                      退出聚焦
                    </button>
                  </div>
                ) : null}
                {groupedDocuments.length > 0 ? (
                  <>
                    <button
                      type="button"
                      onClick={expandAllDocumentGroups}
                      className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
                    >
                      展开全部
                    </button>
                    <button
                      type="button"
                      onClick={collapseAllDocumentGroups}
                      className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-600 transition-colors hover:bg-slate-50"
                    >
                      收起全部
                    </button>
                  </>
                ) : null}
              </div>

              {loadingDocuments ? (
                <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
                  正在加载文档状态列表...
                </div>
              ) : groupedDocuments.length === 0 ? (
                <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-8 text-center text-sm text-slate-400">
                  当前筛选下暂无文档。
                </div>
              ) : (
                <div className="space-y-3">
                  {groupedDocuments.map((group) => (
                    <section key={group.key} className="rounded-2xl border border-slate-200 bg-white">
                      <div className="flex flex-col gap-2 border-b border-slate-100 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
                        <button
                          type="button"
                          onClick={() => toggleDocumentGroup(group.key)}
                          className="flex items-center gap-2 text-left"
                        >
                          {collapsedDocumentGroups[group.key] ? (
                            <ChevronRight className="h-4 w-4 text-slate-400" />
                          ) : (
                            <ChevronDown className="h-4 w-4 text-slate-400" />
                          )}
                          <h4 className="text-sm font-semibold text-slate-800">{group.title}</h4>
                          <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs text-slate-500">
                            {group.items.length} 份
                          </span>
                        </button>
                        <div className="flex flex-wrap items-center gap-2 text-xs">
                          <span className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 text-amber-700">
                            最新 {group.latestCount} 份
                          </span>
                          <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 text-emerald-700">
                            可查看原件 {group.originalCount} 份
                          </span>
                          <span className="rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 text-sky-700">
                            仅提取结果 {Math.max(group.items.length - group.originalCount, 0)} 份
                          </span>
                        </div>
                      </div>

                      {collapsedDocumentGroups[group.key] ? null : (
                        <div className="space-y-3 p-4">
                          {group.items.map((document) => {
                            const highlighted = isHighlightedDocument(document);
                            const focusedFromConflict = focusedConflictDocId === document.doc_id;
                            return (
                            <div
                              key={document.doc_id}
                              data-doc-id={document.doc_id}
                              className={`rounded-2xl border p-4 transition-all ${
                                focusedFromConflict
                                  ? 'border-sky-300 bg-sky-50 shadow-sm shadow-sky-100 ring-2 ring-sky-100'
                                  : highlighted
                                    ? 'border-emerald-300 bg-emerald-50 shadow-sm shadow-emerald-100'
                                  : 'border-slate-200 bg-slate-50/80'
                              }`}
                            >
                              <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                                <div className="min-w-0 flex-1">
                                  <div className="flex flex-wrap items-center gap-2">
                                    <div className="truncate text-sm font-semibold text-slate-800">
                                      {document.file_name || '未命名文件'}
                                    </div>
                                    {focusedFromConflict ? (
                                      <span className="rounded-full border border-sky-200 bg-sky-100 px-2.5 py-1 text-xs font-medium text-sky-700">
                                        来自冲突核验
                                      </span>
                                    ) : null}
                                    {highlighted ? (
                                      <span className="rounded-full border border-emerald-200 bg-emerald-100 px-2.5 py-1 text-xs font-medium text-emerald-700">
                                        刚刚上传
                                      </span>
                                    ) : null}
                                    <span
                                      className={`rounded-full border px-2.5 py-1 text-xs font-medium ${
                                        document.original_available
                                          ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                          : 'border-sky-200 bg-sky-50 text-sky-700'
                                      }`}
                                    >
                                      {document.original_available ? '可查看原件' : '仅保留提取结果'}
                                    </span>
                                    <span
                                      className={`rounded-full border px-2.5 py-1 text-xs font-medium ${
                                        document.is_latest
                                          ? 'border-amber-200 bg-amber-50 text-amber-700'
                                          : 'border-slate-200 bg-white text-slate-500'
                                      }`}
                                    >
                                      {document.is_latest ? '最新' : '历史版本'}
                                    </span>
                                  </div>
                                  <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-500">
                                    <span>资料类型：{getDocumentTypeDisplayLabel(document, extractionGroups)}</span>
                                    <span>上传时间：{formatProfileDateTime(document.upload_time)}</span>
                                    <span>原件状态：{document.original_status}</span>
                                  </div>
                                </div>

                                <div className="flex shrink-0 flex-wrap items-center gap-2">
                                  {document.original_available ? (
                                    <>
                                      <button
                                        type="button"
                                        onClick={() => void handlePreviewDocument(document)}
                                        className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 transition-colors hover:bg-slate-50"
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <Eye className="h-3.5 w-3.5" />
                                          查看原件
                                        </span>
                                      </button>
                                      <button
                                        type="button"
                                        onClick={() => void handleDownloadDocument(document)}
                                        className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 transition-colors hover:bg-slate-50"
                                      >
                                        <span className="inline-flex items-center gap-1">
                                          <Download className="h-3.5 w-3.5" />
                                          下载原件
                                        </span>
                                      </button>
                                    </>
                                  ) : (
                                    <span className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-500">
                                      仅保留提取结果
                                    </span>
                                  )}
                                  </div>
                                </div>
                            </div>
                          );
                          })}
                        </div>
                      )}
                    </section>
                  ))}
                </div>
              )}
            </div>
          </section>
        ) : null}

        {!selectedCustomerId ? (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-400">请选择客户</div>
        ) : loadingProfile ? (
          <div className="flex flex-1 items-center justify-center text-sm text-slate-400">加载资料汇总中...</div>
        ) : (
          <div className="grid grid-cols-1 xl:grid-cols-2">
            <section className="flex flex-col border-b border-slate-200 bg-white xl:border-b-0 xl:border-r">
              <div className="border-b border-slate-200 px-5 py-3">
                <div className="inline-flex items-center gap-2 rounded-full bg-blue-50 px-3 py-1.5 text-sm font-medium text-blue-700">
                  <FileText className="h-4 w-4" />
                  资料内容
                </div>
              </div>
              {mode === 'edit' ? (
                <textarea
                  value={renderedDraft}
                  onChange={(e) => setDraft(e.target.value)}
                  className="min-h-[520px] resize-y border-0 p-5 font-mono text-sm leading-6 text-slate-700 outline-none"
                  placeholder="请输入资料汇总内容，支持标题、分段等格式"
                />
              ) : (
                <div className="overflow-visible p-5">
                  <pre className="whitespace-pre-wrap break-words text-sm leading-6 text-slate-700">
                    {draft || '暂无内容'}
                  </pre>
                </div>
              )}
            </section>

            <section className="flex flex-col bg-[radial-gradient(circle_at_top,_rgba(148,163,184,0.14),_transparent_45%),linear-gradient(180deg,#f8fafc_0%,#f1f5f9_100%)]">
              <div className="border-b border-slate-200 px-5 py-3">
                <div className="inline-flex items-center gap-2 rounded-full bg-slate-100 px-3 py-1.5 text-sm font-medium text-slate-700">
                  <Eye className="h-4 w-4" />
                  阅读预览
                </div>
              </div>
              <div className="overflow-visible px-6 py-6">
                <article className="prose prose-slate max-w-none rounded-[28px] border border-white/80 bg-white/95 p-7 shadow-[0_18px_45px_rgba(15,23,42,0.08)] backdrop-blur">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{renderedDraft || '暂无内容'}</ReactMarkdown>
                </article>
              </div>
            </section>
          </div>
        )}
      </main>
    </div>
  );
};

export default CustomerDataPage;

