import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ProductMatchingPanel } from './ProductMatchingPanel';
import { confirmFinancingPlan, createFinancingPlanFromCombination, createFinancingPlanSelection, createManualCandidateOverride, generateFinancingPlanCombinations, generateFinancingPlanReport, getCustomerFinancingReview, getLatestProductMatching, listCustomerFinancingPlans, listFinancingApplications, runProductMatching, updateFinancingPlan, updateFinancingPlanCondition, updateFinancingPlanMaterial, validateFinancingPlan, type FinancingRequirementData, type ProductMatchingSnapshotData } from '../services/api';

vi.mock('../services/api', () => ({
  getLatestProductMatching: vi.fn(),
  runProductMatching: vi.fn(),
  generateFinancingPlanCombinations: vi.fn(),
  createFinancingPlanFromCombination: vi.fn(),
  createManualCandidateOverride: vi.fn(),
  updateFinancingPlan: vi.fn(),
  updateFinancingPlanCondition: vi.fn(),
  updateFinancingPlanMaterial: vi.fn(),
  validateFinancingPlan: vi.fn(),
  confirmFinancingPlan: vi.fn(),
  createFinancingPlanSelection: vi.fn(),
  finalizeFinancingPlanSelection: vi.fn(),
  compareFinancingPlanVersions: vi.fn(),
  generateFinancingPlanReport: vi.fn(),
  downloadFinancingPlanReportPdf: vi.fn(),
  listCustomerFinancingPlans: vi.fn(),
  listFinancingApplications: vi.fn(),
  createFinancingApplications: vi.fn(),
  startApplicationPreparation: vi.fn(),
  markApplicationReady: vi.fn(),
  submitFinancingApplication: vi.fn(),
  markApplicationUnderReview: vi.fn(),
  requestApplicationSupplement: vi.fn(),
  recordApplicationApproval: vi.fn(),
  recordApplicationRejection: vi.fn(),
  recordApplicationDisbursement: vi.fn(),
  retryFinancingApplication: vi.fn(),
  updateFinancingApplicationTask: vi.fn(),
  getCustomerFinancingReview: vi.fn(), getFinancingApplicationOutcome: vi.fn(),
  finalizeFinancingApplicationOutcome: vi.fn(),
}));

const requirement: FinancingRequirementData = {
  requirement_id: 'req-1', customer_id: 'customer-1', version: 1, status: 'confirmed',
  borrower_entity: '上海意川建筑科技有限公司', requested_amount: 8_000_000,
  amount_confirmed: true, financing_purpose: '采购', purpose_detail: '材料采购',
  term_value: 12, term_unit: 'month', term_confirmed: true,
};

const snapshot: ProductMatchingSnapshotData = {
  status: 'completed', snapshot_id: 'snapshot-1', customer_id: 'customer-1', requirement_id: 'req-1',
  requirement_version: 1, generated_at: '2026-09-24T10:00:00', catalog_as_of_date: '2026-09-24',
  summary: { eligible: 1, conditional: 0, ineligible: 0, manual_review: 1, product_configuration_error: 0, total: 2 },
  data_quality: {
    missing_domains: ['asset', 'business', 'qualification'],
    preliminary_fields: [
      'cashflow.operating_inflow',
      'cashflow.non_operating_or_unidentified_inflow',
      'cashflow.average_monthly_operating_inflow',
    ],
  },
  items: [
    { product_match_item_id: 1, product_id: 'p1', version_id: 'v1', external_product_code: 'A-001', institution_name: '甲银行', product_name: '经营贷', product_category: 'enterprise_credit', max_amount: '10000000', max_term_months: 12, overall_status: 'eligible', blocking_reasons: [], missing_information: [], review_reasons: [], soft_gaps: [], rule_results: [{ rule_id: 'r1', rule_source: 'product_rule', field_name: 'requirement.amount', result: 'passed', explanation: '本次融资金额满足产品要求。' }] },
    { product_match_item_id: 2, product_id: 'p2', version_id: 'v2', external_product_code: 'B-001', institution_name: '乙银行', product_name: '科创贷', product_category: 'technology_enterprise', max_amount: null, max_term_months: null, overall_status: 'manual_review', blocking_reasons: [], missing_information: [], review_reasons: ['该产品尚未配置足够结构化准入规则，暂不能自动判断。'], soft_gaps: [], rule_results: [] },
  ],
};

describe('ProductMatchingPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    vi.mocked(getLatestProductMatching).mockResolvedValue(null);
    vi.mocked(listCustomerFinancingPlans).mockResolvedValue([]);
    vi.mocked(listFinancingApplications).mockResolvedValue([]);
    vi.mocked(getCustomerFinancingReview).mockResolvedValue({ customer_id: 'customer-1', requirement_id: 'req-1', requirement_version: 1, requirement: { amount: '8000000', purpose: '材料采购', term_value: 12, term_unit: 'month' }, application_count: 0, outcome_count: 0, submitted_amount: '0.00', approved_amount: '0.00', disbursed_amount: '0.00', funding_gap: '8000000.00', actual_coverage_ratio: '0.0000', execution_status: 'not_started', message: '当前尚无已执行融资申请。', rejection_reasons: [], rule_feedback: [] });
  });

  it('groups products by business status without ranking', async () => {
    vi.mocked(runProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    fireEvent.click(screen.getByText('开始产品匹配'));
    await waitFor(() => expect(runProductMatching).toHaveBeenCalledWith('customer-1', 'req-1'));
    expect(await screen.findByText('符合当前已知硬条件：1')).toBeTruthy();
    expect(screen.getByText('需要人工复核：1')).toBeTruthy();
    expect(screen.queryByText(/Top|推荐指数|评分/)).toBeNull();
  });

  it('shows data quality and the no active catalog message', async () => {
    vi.mocked(runProductMatching).mockResolvedValue({
      status: 'no_active_products', snapshot_id: null,
      message: '当前产品库尚无已发布产品，请先完成产品发布。',
      summary: { eligible: 0, conditional: 0, ineligible: 0, manual_review: 0, product_configuration_error: 0, total: 0 },
      data_quality: {}, items: [],
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    fireEvent.click(screen.getByText('开始产品匹配'));
    expect(await screen.findByText('当前产品库尚无已发布产品，请先完成产品发布。')).toBeTruthy();
  });

  it('restores the latest snapshot for the same requirement', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    expect(await screen.findByText('重新匹配')).toBeTruthy();
    expect(screen.getByText('甲银行 · 经营贷')).toBeTruthy();
  });

  it('renders data quality field labels in Chinese', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    expect(await screen.findByText(/初步分类字段：经营流入、其他\/未识别流入、月均经营流入/)).toBeTruthy();
  });

  it('uses the Chinese label for non-operating inflow', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    expect(await screen.findByText(/其他\/未识别流入/)).toBeTruthy();
  });

  it('uses the Chinese label for average monthly operating inflow', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    expect(await screen.findByText(/月均经营流入/)).toBeTruthy();
  });

  it('does not render an unmapped raw fact path', async () => {
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    vi.mocked(getLatestProductMatching).mockResolvedValue({
      ...snapshot,
      data_quality: { conflicted_fields: ['credit.personal.future_metric'] },
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    expect(await screen.findByText('冲突字段：未命名字段')).toBeTruthy();
    expect(screen.queryByText(/credit\.personal\.future_metric/)).toBeNull();
    expect(warning).toHaveBeenCalledWith(expect.stringContaining('credit.personal.future_metric'));
    warning.mockRestore();
  });

  it('does not auto-create a plan when no eligible or conditional product exists', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue({
      ...snapshot,
      summary: { eligible: 0, conditional: 0, ineligible: 3, manual_review: 2, product_configuration_error: 0, total: 5 },
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    const button = await screen.findByRole('button', { name: '生成融资方案' });
    expect((button as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText('当前尚无可自动形成的融资方案。')).toBeTruthy();
    expect(screen.getByText(/需要人工复核产品：2 · 已排除产品：3 · 未覆盖金额：800万元/)).toBeTruthy();
    expect(generateFinancingPlanCombinations).not.toHaveBeenCalled();
  });

  it('renders deterministic combination results and creates only a draft', async () => {
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    vi.mocked(generateFinancingPlanCombinations).mockResolvedValue({
      customer_id: 'customer-1', requirement_id: 'req-1', requirement_version: 1,
      source_match_snapshot_id: 'snapshot-1', target_amount: '8000000', covered_amount: '8000000', funding_gap: '0',
      currency: 'CNY', generation_status: 'complete', conditional_combinations: [], partial_combinations: [],
      manual_review_candidates: [], excluded_candidates: [],
      formal_combinations: [{
        combination_id: 'combo-1', plan_type: 'primary_candidate', generation_status: 'complete',
        target_amount: '8000000', covered_amount: '8000000', funding_gap: '0', product_count: 1, institution_count: 1,
        institution_concentration: false, conditions: [], missing_information: [],
        risks: ['方案金额为规划金额，不代表银行最终批复金额。'], gaps: [], is_complete: true,
        requires_manual_review: false, source_match_snapshot_id: 'snapshot-1',
        items: [{
          product_match_item_id: 1, product_id: 'p1', product_version_id: 'v1', external_product_code: 'A-001',
          institution_name: '甲银行', product_name: '经营贷', match_status: 'eligible', candidate_status: 'usable', manual_override_id: null, proposed_amount: '8000000',
          proposed_term_months: 12, allocation_cap: '10000000', min_amount: null,
          conditions: [], missing_information: [], review_reasons: [],
        }],
      }],
    });
    vi.mocked(createFinancingPlanFromCombination).mockResolvedValue({
      financing_plan_id: 'plan-1', customer_id: 'customer-1', requirement_id: 'req-1', requirement_version: 1,
      source_match_snapshot_id: 'snapshot-1', current_version_id: 'pv-1', status: 'draft', versions: [{
        plan_version_id: 'pv-1', version_no: 1, plan_type: 'primary', target_amount: '8000000', covered_amount: '8000000', funding_gap: '0', status: 'draft', generation_status: 'complete',
        items: [{ product_match_item_id: 1, institution_name: '甲银行', product_name: '经营贷', proposed_amount: '8000000', proposed_term_months: 12, item_role: 'primary', reason: '', notes: '', conditions: [], risks: [] }],
        gaps: [], condition_checklist: [], material_checklist: [], explanation: { plan_summary: '目标融资800万元。', coverage_summary: '覆盖800万元。', funding_gap_summary: '无缺口。', key_conditions: [], risk_notes: [], next_actions: [] },
      }],
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    fireEvent.click(await screen.findByRole('button', { name: '生成融资方案' }));
    expect(await screen.findByText('主方案候选')).toBeTruthy();
    expect(screen.getByText(/规划金额：800万元/)).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: '选择此方案' }));
    expect(await screen.findByText('方案草稿已创建：plan-1')).toBeTruthy();
    expect(createFinancingPlanFromCombination).toHaveBeenCalledWith('customer-1', 'req-1', 'snapshot-1', 'combo-1');
  });

  it('shows manual review candidates and records an explicit conditional override', async () => {
    const noCombinationSnapshot = {
      ...snapshot,
      summary: { eligible: 0, conditional: 0, ineligible: 3, manual_review: 2, product_configuration_error: 0, total: 5 },
      items: snapshot.items.filter(item => item.overall_status === 'manual_review'),
    };
    vi.mocked(getLatestProductMatching).mockResolvedValue(noCombinationSnapshot);
    vi.mocked(createManualCandidateOverride).mockResolvedValue({ override_id: 'override-1', new_status: 'conditional' });
    vi.mocked(generateFinancingPlanCombinations).mockResolvedValue({
      customer_id: 'customer-1', requirement_id: 'req-1', requirement_version: 1, source_match_snapshot_id: 'snapshot-1',
      target_amount: '8000000', covered_amount: '0', funding_gap: '8000000', currency: 'CNY', generation_status: 'manual_review_required',
      formal_combinations: [], conditional_combinations: [], partial_combinations: [], manual_review_candidates: [], excluded_candidates: [],
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    fireEvent.click(await screen.findByRole('button', { name: '查看人工复核候选' }));
    expect(screen.getAllByText('乙银行 · 科创贷').length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByText('房产证')).toBeNull();
    fireEvent.change(screen.getByLabelText('科创贷人工纳入原因'), { target: { value: '已核验额度与期限边界' } });
    fireEvent.click(screen.getByRole('button', { name: '纳入条件性候选' }));
    await waitFor(() => expect(createManualCandidateOverride).toHaveBeenCalledWith('customer-1', 'req-1', 'snapshot-1', 2, '已核验额度与期限边界'));
    expect(await screen.findByText('人工复核决定已记录，产品已作为条件性候选重新参与组合。')).toBeTruthy();
  });

  it('previews a truthful customer report without a primary plan', async () => {
    localStorage.setItem('auth_role', 'operator');
    vi.mocked(getLatestProductMatching).mockResolvedValue({
      ...snapshot,
      summary: { eligible: 0, conditional: 0, ineligible: 3, manual_review: 2, product_configuration_error: 0, total: 5 },
    });
    vi.mocked(createFinancingPlanSelection).mockResolvedValue({
      selection_id: 'selection-1', customer_id: 'customer-1', requirement_id: 'req-1',
      primary_plan_version_id: null, backup_plan_version_ids: [], conditional_plan_version_ids: [],
      status: 'draft', selected_by: 'operator', selected_at: '2026-09-29T10:00:00', notes: '',
    });
    vi.mocked(generateFinancingPlanReport).mockResolvedValue({
      report_id: 'report-1', plan_selection_id: 'selection-1', report_type: 'customer', report_version: 1,
      structured_payload: {}, rendered_html: '<h1>融资规划方案</h1><p>暂未形成正式主方案</p>', generated_at: '2026-09-29T10:00:00',
    });
    render(<ProductMatchingPanel requirement={requirement} />);
    fireEvent.click(await screen.findByRole('button', { name: '客户版预览' }));
    await waitFor(() => expect(createFinancingPlanSelection).toHaveBeenCalledWith(expect.objectContaining({ primary_plan_version_id: null })));
    expect(await screen.findByTitle('融资方案报告预览')).toBeTruthy();
    expect(screen.getByText(/当前产品库中尚无可直接形成正式融资方案/)).toBeTruthy();
  });

  it('keeps selection and report generation read-only for viewer', async () => {
    localStorage.setItem('auth_role', 'viewer');
    vi.mocked(getLatestProductMatching).mockResolvedValue(snapshot);
    render(<ProductMatchingPanel requirement={requirement} />);
    await screen.findByText('方案定稿与报告');
    expect(screen.queryByRole('button', { name: '客户版预览' })).toBeNull();
    expect(screen.queryByRole('button', { name: '内部版预览' })).toBeNull();
  });
});
