import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FinancingExecutionPanel } from './FinancingExecutionPanel';
import { completeFinancingApplicationTask, createApplicationPackage, createFinancingApplications, getApplicationMaterials, getCustomerFinancingReview, getFinancingApplicationOutcome, listApplicationPackages, listFinancingApplications, matchApplicationMaterials, type FinancingApplicationData, type FinancingPlanData, type FinancingRequirementData } from '../services/api';

vi.mock('../services/api', () => ({
  listFinancingApplications: vi.fn(), createFinancingApplications: vi.fn(),
  startApplicationPreparation: vi.fn(), markApplicationReady: vi.fn(), submitFinancingApplication: vi.fn(),
  markApplicationUnderReview: vi.fn(), requestApplicationSupplement: vi.fn(), recordApplicationApproval: vi.fn(),
  recordApplicationRejection: vi.fn(), recordApplicationDisbursement: vi.fn(), updateFinancingApplicationTask: vi.fn(),
  completeFinancingApplicationTask: vi.fn(), blockFinancingApplicationTask: vi.fn(), unblockFinancingApplicationTask: vi.fn(),
  completeApplicationSupplement: vi.fn(), updateApplicationMaterial: vi.fn(), updateApplicationApprovalCondition: vi.fn(),
  addApplicationReviewFeedback: vi.fn(), createApplicationDisbursement: vi.fn(),
  retryFinancingApplication: vi.fn(),
  getApplicationMaterials: vi.fn(), listApplicationPackages: vi.fn(), matchApplicationMaterials: vi.fn(),
  verifyApplicationMaterial: vi.fn(), createApplicationPackage: vi.fn(), freezeApplicationPackage: vi.fn(),
  createSubmissionPackage: vi.fn(), createSupplementPackage: vi.fn(), downloadSubmissionPackageZip: vi.fn(),
  getApplicationMaterialExtraction: vi.fn(), replaceApplicationMaterial: vi.fn(), selectApplicationMaterial: vi.fn(),
  listApplicationContacts: vi.fn(), listApplicationCommunications: vi.fn(), listFinancingFollowUps: vi.fn(),
  getFinancingApplicationTimeline: vi.fn(), createFinancingContact: vi.fn(), attachApplicationContact: vi.fn(),
  createApplicationCommunication: vi.fn(), createTaskFromCommunication: vi.fn(),
  createSupplementFromCommunication: vi.fn(), createReviewFeedbackFromCommunication: vi.fn(),
  completeFinancingFollowUp: vi.fn(),
  getCustomerFinancingReview: vi.fn(), getFinancingApplicationOutcome: vi.fn(),
  finalizeFinancingApplicationOutcome: vi.fn(),
}));

const requirement = { requirement_id: 'r1', customer_id: 'c1', version: 2, status: 'confirmed', borrower_entity: '上海意川建筑科技有限公司', requested_amount: 8_000_000, amount_confirmed: true, financing_purpose: '采购', purpose_detail: '材料采购', term_value: 12, term_unit: 'month', term_confirmed: true } as FinancingRequirementData;
const version = { plan_version_id: 'pv1', version_no: 1, plan_type: 'primary', target_amount: '8000000', covered_amount: '8000000', funding_gap: '0', status: 'confirmed', generation_status: 'complete', items: [], gaps: [], condition_checklist: [], material_checklist: [], explanation: null };
const plan = { financing_plan_id: 'p1', customer_id: 'c1', requirement_id: 'r1', requirement_version: 2, source_match_snapshot_id: 's1', current_version_id: 'pv1', status: 'confirmed', versions: [version] } as FinancingPlanData;
const application: FinancingApplicationData = {
  application_id: 'a1', parent_application_id: null, attempt_no: 1, customer_id: 'c1', requirement_id: 'r1', requirement_version: 2,
  plan_id: 'p1', plan_version_id: 'pv1', plan_item_id: 'pi1', product_id: 'product1', product_version_id: 'product-v1',
  external_product_code: 'ABC-001', institution_name: '农业银行', product_name: '测试产品', application_no: 'FA-1', status: 'preparing',
  target_amount: '8000000', submitted_amount: null, approved_amount: null, disbursed_amount: null, target_term_months: 12,
  approved_term_months: null, target_interest_rate: null, approved_interest_rate: null, responsible_user_id: null, responsible_user_name: null,
  current_stage_code: '01_material_preparation', submission_channel: null, submission_reference: null, approval_reference: null,
  rejection_reason: '', disbursement_reference: null, final_result: null,
  stages: [{ stage_id: 'stage1', stage_code: '01_material_preparation', stage_name: '材料准备', status: 'in_progress', sequence_no: 1 }],
  tasks: [{ task_id: 't1', stage_id: 'stage1', task_type: 'material', title: '准备：营业执照', description: '', status: 'todo', priority: 'normal', assignee_user_name: null, due_date: null, source_type: 'plan_material', source_ref: 'm1', required: true, notes: '' }],
  events: [{ event_id: 'e1', event_type: 'application_created', from_status: null, to_status: 'draft', operator_name: '经办人', payload: {}, created_at: '2026-09-29T10:00:00' }],
};

describe('FinancingExecutionPanel', () => {
  beforeEach(async () => { vi.clearAllMocks(); localStorage.setItem('auth_role', 'operator'); vi.mocked(listFinancingApplications).mockResolvedValue([]); vi.mocked(getFinancingApplicationOutcome).mockResolvedValue(null); vi.mocked(getCustomerFinancingReview).mockResolvedValue({ customer_id: 'c1', requirement_id: 'r1', requirement_version: 2, requirement: { amount: '8000000', purpose: '材料采购', term_value: 12, term_unit: 'month' }, application_count: 0, outcome_count: 0, submitted_amount: '0.00', approved_amount: '0.00', disbursed_amount: '0.00', funding_gap: '8000000.00', actual_coverage_ratio: '0.0000', execution_status: 'not_started', message: '当前尚无已执行融资申请。', rejection_reasons: [], rule_feedback: [] }); vi.mocked(getApplicationMaterials).mockResolvedValue({ application_id: 'a1', materials: [], summary: { total_required: 0, verified_count: 0, available_count: 0, missing_count: 0, expired_count: 0, review_count: 0 }, events: [] }); vi.mocked(listApplicationPackages).mockResolvedValue({ packages: [], submission_packages: [], supplement_packages: [] }); const api = await import('../services/api'); vi.mocked(api.listApplicationContacts).mockResolvedValue([]); vi.mocked(api.listApplicationCommunications).mockResolvedValue([]); vi.mocked(api.listFinancingFollowUps).mockResolvedValue([]); vi.mocked(api.getFinancingApplicationTimeline).mockResolvedValue([]); });

  it('shows Shanghai Yichuan empty state without creating an application', async () => {
    render(<FinancingExecutionPanel requirement={requirement} plans={[]} />);
    expect(await screen.findByText('当前没有可创建融资申请的已确认方案。')).toBeTruthy();
    expect(screen.getByText('当前暂无融资申请，创建申请后可记录银行及客户沟通。')).toBeTruthy();
    expect(screen.getByText('申请产品：0个')).toBeTruthy();
    expect(await screen.findByText(/当前尚无已执行融资申请/)).toBeTruthy();
    expect(createFinancingApplications).not.toHaveBeenCalled();
  });

  it('creates applications only after an explicit confirmed plan selection', async () => {
    vi.mocked(createFinancingApplications).mockResolvedValue([application]);
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    fireEvent.change(await screen.findByLabelText('选择已确认方案版本'), { target: { value: 'pv1' } });
    fireEvent.click(screen.getByRole('button', { name: '创建融资申请' }));
    await waitFor(() => expect(createFinancingApplications).toHaveBeenCalledWith('c1', 'pv1'));
    expect((await screen.findAllByText('农业银行 · 测试产品')).length).toBeGreaterThanOrEqual(1);
  });

  it('renders stages, tasks and event timeline', async () => {
    vi.mocked(listFinancingApplications).mockResolvedValue([application]);
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    expect(await screen.findByText('阶段进度')).toBeTruthy();
    expect(screen.getByText(/准备：营业执照 · 方案材料清单/)).toBeTruthy();
    expect(screen.getByText(/创建融资申请 · 经办人/)).toBeTruthy();
  });

  it('lets an operator complete a sourced task', async () => {
    vi.mocked(listFinancingApplications).mockResolvedValue([application]);
    vi.mocked(completeFinancingApplicationTask).mockResolvedValue({ ...application, tasks: [{ ...application.tasks[0], status: 'done' }] });
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    fireEvent.click(await screen.findByRole('button', { name: '完成' }));
    await waitFor(() => expect(completeFinancingApplicationTask).toHaveBeenCalledWith('a1', 't1'));
  });

  it('shows material management and creates package only by explicit action', async () => {
    vi.mocked(listFinancingApplications).mockResolvedValue([application]);
    vi.mocked(matchApplicationMaterials).mockResolvedValue({ application_id: 'a1', results: [], summary: { total_required: 0, verified_count: 0, available_count: 0, missing_count: 0, expired_count: 0, review_count: 0 } });
    vi.mocked(createApplicationPackage).mockResolvedValue({ package_id: 'pkg1', application_id: 'a1', package_version: 1, status: 'ready', created_by: 'operator', created_at: null, items: [] });
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    expect(await screen.findByText('材料管理')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: '匹配现有资料' }));
    await waitFor(() => expect(matchApplicationMaterials).toHaveBeenCalledWith('a1'));
    fireEvent.click(screen.getByRole('button', { name: '生成进件材料包' }));
    await waitFor(() => expect(createApplicationPackage).toHaveBeenCalledWith('a1'));
  });

  it('renders Chinese material status and disables packaging while required material is missing', async () => {
    vi.mocked(listFinancingApplications).mockResolvedValue([application]);
    vi.mocked(getApplicationMaterials).mockResolvedValue({ application_id: 'a1', materials: [{
      application_material_id: 'am1', supplement_request_id: null, material_type: 'business_license',
      material_name: '营业执照', material_category: 'enterprise', owner_type: 'enterprise', owner_id: null,
      owner_name: '上海意川建筑科技有限公司', required: true, required_verified: false,
      status: 'required_missing', source_type: 'plan_material', source_id: 'pm1', source_document_id: null,
      customer_material_id: null, source_file_id: null, file_reference: null, valid_from: null, valid_to: null,
      coverage_start: null, coverage_end: null, version_no: 1, replaces_material_id: null,
      rejection_reason: '', verified_by: null, verified_at: null, notes: '',
    }], summary: { total_required: 1, verified_count: 0, available_count: 0, missing_count: 1, expired_count: 0, review_count: 0 }, events: [] });
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    expect(await screen.findByText(/营业执照 · 营业执照/)).toBeTruthy();
    expect(screen.getAllByText('缺失').length).toBeGreaterThan(0);
    expect(screen.queryByText('required_missing')).toBeNull();
    expect(screen.getByRole('button', { name: '生成进件材料包' })).toHaveProperty('disabled', true);
  });

  it('keeps viewer material management read only', async () => {
    localStorage.setItem('auth_role', 'viewer'); vi.mocked(listFinancingApplications).mockResolvedValue([application]);
    render(<FinancingExecutionPanel requirement={requirement} plans={[plan]} />);
    expect(await screen.findByText('材料管理')).toBeTruthy();
    expect(screen.queryByRole('button', { name: '匹配现有资料' })).toBeNull();
    expect(screen.queryByRole('button', { name: '生成进件材料包' })).toBeNull();
  });
});
