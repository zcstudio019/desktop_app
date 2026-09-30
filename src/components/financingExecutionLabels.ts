export const APPLICATION_STATUS_LABELS: Record<string, string> = {
  draft: '草稿', preparing: '材料准备中', ready_to_submit: '待提交', submitted: '已提交',
  supplement_required: '需补件', under_review: '审批中', approved: '已批复', rejected: '已拒绝',
  partially_approved: '部分批复', disbursing: '放款中', disbursed: '已放款', cancelled: '已取消', closed: '已关闭',
};

export const APPLICATION_STAGE_STATUS_LABELS: Record<string, string> = {
  pending: '待开始', in_progress: '进行中', completed: '已完成', skipped: '已跳过', blocked: '已阻塞',
};

export const APPLICATION_EVENT_LABELS: Record<string, string> = {
  application_created: '创建融资申请', status_changed: '申请状态变更', task_created: '创建执行任务',
  task_completed: '完成执行任务', task_status_changed: '更新任务状态', submitted: '正式提交银行',
  supplement_requested: '银行要求补件', supplement_completed: '完成补件', application_material_updated: '更新申请材料',
  review_feedback_added: '记录审批反馈', approval_recorded: '记录批复', rejected: '记录拒绝',
  approval_condition_updated: '更新批复条件', partial_disbursement_recorded: '记录部分放款', disbursed: '完成放款',
  contact_attached: '关联联系人', communication_recorded: '记录沟通',
  communication_voided: '作废沟通记录', follow_up_created: '创建跟进事项',
  follow_up_updated: '更新跟进事项',
  outcome_finalized: '定稿申请最终结果', outcome_corrected: '修正申请最终结果',
  application_closed: '关闭融资申请', rule_feedback_created: '提交产品规则反馈',
};

export const APPLICATION_TASK_SOURCE_LABELS: Record<string, string> = {
  plan_material: '方案材料清单', plan_condition: '方案条件清单', bank_supplement: '银行补件要求',
  supplement_request: '补件要求', communication: '沟通记录',
};

export const APPLICATION_TASK_STATUS_LABELS: Record<string, string> = {
  todo: '待处理', in_progress: '处理中', blocked: '已阻塞', done: '已完成', cancelled: '已取消',
};

export const APPLICATION_MATERIAL_STATUS_LABELS: Record<string, string> = {
  required_missing: '缺失', missing: '缺失', matched: '已匹配', available: '已有',
  uploaded: '已上传', verified: '已核验', expired: '已过期', needs_review: '待核验',
  rejected: '不采用', not_applicable: '不适用',
};

export const MATERIAL_TYPE_LABELS: Record<string, string> = {
  business_license: '营业执照', id_card: '身份证', marriage_certificate: '结婚证',
  real_estate_certificate: '房产证', enterprise_credit_report: '企业征信报告',
  personal_credit_report: '个人征信报告', enterprise_bank_statement: '企业银行流水',
  personal_bank_statement: '个人银行流水', financial_statement: '财务报表',
  account_opening_permit: '开户许可证', company_articles: '公司章程', shareholder_info: '股东信息',
  tax_record: '纳税资料', invoice_record: '开票资料', other: '其他',
};

export const MATERIAL_PACKAGE_STATUS_LABELS: Record<string, string> = {
  draft: '草稿', ready: '已就绪', frozen: '已冻结', submitted: '已提交', superseded: '已被新版本替代',
};

export const COMMUNICATION_SIDE_LABELS: Record<string, string> = {
  customer: '客户', institution: '金融机构', internal: '内部沟通',
};

export const COMMUNICATION_CHANNEL_LABELS: Record<string, string> = {
  phone: '电话', wechat: '微信', email: '邮件', meeting: '会议', onsite: '现场', system: '系统记录', other: '其他',
};

export const COMMUNICATION_DIRECTION_LABELS: Record<string, string> = {
  inbound: '对方联系我方', outbound: '我方联系对方', internal: '内部沟通',
};

export const COMMUNICATION_OUTCOME_LABELS: Record<string, string> = {
  info_only: '仅记录', waiting_customer: '等待客户', waiting_institution: '等待机构',
  action_required: '需要处理', resolved: '已解决',
};

export const FOLLOW_UP_STATUS_LABELS: Record<string, string> = {
  pending: '待跟进', in_progress: '跟进中', completed: '已完成', cancelled: '已取消',
};
