import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ProductMatchingPanel } from './ProductMatchingPanel';
import { getLatestProductMatching, runProductMatching, type FinancingRequirementData, type ProductMatchingSnapshotData } from '../services/api';

vi.mock('../services/api', () => ({
  getLatestProductMatching: vi.fn(),
  runProductMatching: vi.fn(),
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
  data_quality: { missing_domains: ['asset', 'business', 'qualification'], preliminary_fields: ['cashflow.operating_inflow'] },
  items: [
    { product_id: 'p1', version_id: 'v1', external_product_code: 'A-001', institution_name: '甲银行', product_name: '经营贷', product_category: 'enterprise_credit', max_amount: '10000000', max_term_months: 12, overall_status: 'eligible', blocking_reasons: [], missing_information: [], review_reasons: [], soft_gaps: [], rule_results: [{ rule_id: 'r1', rule_source: 'product_rule', field_name: 'requirement.amount', result: 'passed', explanation: '本次融资金额满足产品要求。' }] },
    { product_id: 'p2', version_id: 'v2', external_product_code: 'B-001', institution_name: '乙银行', product_name: '科创贷', product_category: 'technology_enterprise', max_amount: null, max_term_months: null, overall_status: 'manual_review', blocking_reasons: [], missing_information: [], review_reasons: ['该产品尚未配置足够结构化准入规则，暂不能自动判断。'], soft_gaps: [], rule_results: [] },
  ],
};

describe('ProductMatchingPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getLatestProductMatching).mockResolvedValue(null);
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
});
