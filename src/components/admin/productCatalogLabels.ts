export const PRODUCT_FIELD_LABELS: Record<string, string> = {
  external_product_code: '产品编号', institution_name: '机构名称', product_name: '产品名称', product_category: '产品分类',
  max_amount: '最高额度', max_term_months: '最长期限', region_scope: '适用地区', region_scope_json: '适用地区',
  guarantee_modes: '担保方式', guarantee_modes_json: '担保方式', guarantee_type: '担保类型',
  collateral_types: '抵押物类型', collateral_types_json: '抵押物类型', materials: '材料清单', materials_json: '材料清单',
  company_age_rule: '企业成立年限要求', company_age_months: '企业成立月数', loan_type: '贷款类型', rate_text: '利率说明',
  min_amount: '最低额度', min_term_months: '最短期限', repayment_methods_json: '还款方式', borrower_age_min: '借款人最小年龄',
  borrower_age_max: '借款人最大年龄', tax_grade: '纳税等级', revenue_requirement: '营收要求', tax_requirement: '纳税要求',
  invoice_requirement: '开票要求', credit_overdue_requirement: '征信逾期要求', credit_query_requirement: '征信查询要求',
  debt_requirement: '负债要求', collateral_requirement: '抵押物要求', suitable_customer_text: '适合客户', notes: '备注',
  effective_from: '生效日期', effective_to: '失效日期', raw_fields_json: '来源原始字段', summary: '产品摘要', currency: '币种',
};

export const REVIEW_STATUS_LABELS: Record<string, string> = {
  extracted_review: '已提取，待人工确认', needs_review: '需要人工复核', insufficient_data: '资料不足',
  reviewed: '已确认', confirmed: '已确认', rejected: '不采用', not_applicable: '不适用',
  acknowledged_unknown: '资料不足', unreviewed: '未审核', reviewing: '审核中',
};

export const VERSION_STATUS_LABELS: Record<string, string> = {
  draft: '草稿', needs_review: '待审核', published: '已发布', superseded: '已被新版本替代', expired: '已过期', disabled: '已停用',
};

export const PRODUCT_CATEGORY_LABELS: Record<string, string> = {
  guarantee_fund: '担保基金', personal_mortgage: '个人抵押', personal_credit: '个人信用',
  technology_enterprise: '科技企业', enterprise_mortgage: '企业抵押', enterprise_credit: '企业信用',
  personal: '个人产品', other: '其他',
};

export const RULE_SEVERITY_LABELS: Record<string, string> = { hard: '硬性规则', soft: '软性规则', info: '提示规则' };
export const RULE_ACTION_LABELS: Record<string, string> = { exclude: '排除', conditional: '条件性通过', review: '人工复核' };
export const RULE_OPERATOR_LABELS: Record<string, string> = {
  eq: '等于', ne: '不等于', in: '属于', not_in: '不属于', gt: '大于', gte: '大于等于', lt: '小于', lte: '小于等于',
  between: '介于', contains: '包含', exists: '有值', not_exists: '无值',
};
export const RULE_FIELD_LABELS: Record<string, string> = {
  'requirement.amount': '融资需求－金额', 'requirement.purpose': '融资需求－用途', 'requirement.term_months': '融资需求－期限',
  'requirement.accept_mortgage': '融资需求－接受抵押', 'requirement.accept_additional_guarantee': '融资需求－接受追加担保',
  'requirement.registered_region': '融资需求－注册地区', 'requirement.operating_region': '融资需求－经营地区',
  'customer.company_age_months': '企业－成立月数', 'customer.registered_region': '企业－注册地区',
  'customer.operating_region': '企业－经营地区', 'customer.enterprise_type': '企业－主体类型',
  'credit.enterprise_overdue_count': '征信－企业逾期次数', 'credit.personal_overdue_count': '征信－个人逾期次数',
  'credit.overdue_90d_count': '征信－90天以上逾期次数', 'credit.enterprise_outstanding_balance': '征信－企业未结清余额',
  'credit.personal_outstanding_balance': '征信－个人未结清余额', 'credit.related_repayment_balance': '征信－关联还款余额',
  'financial.total_assets': '财务－总资产', 'financial.total_liabilities': '财务－总负债', 'financial.net_assets': '财务－净资产',
  'financial.debt_asset_ratio': '财务－资产负债率', 'financial.revenue': '财务－营业收入', 'financial.net_profit': '财务－净利润',
  'cashflow.coverage_months': '流水－覆盖月数', 'cashflow.operating_inflow': '流水－经营流入',
  'cashflow.average_monthly_operating_inflow': '流水－月均经营流入', 'asset.has_real_estate': '资产－有房产',
  'asset.has_vehicle': '资产－有车辆', 'asset.has_equipment': '资产－有设备', 'asset.has_confirmed_collateral': '资产－有已确认抵押物',
};

export function productFieldLabel(fieldName: string): string {
  if (fieldName.startsWith('raw_fields.')) return `来源字段：${fieldName.slice('raw_fields.'.length)}`;
  return PRODUCT_FIELD_LABELS[fieldName] || RULE_FIELD_LABELS[fieldName] || '其他产品字段';
}
