import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import FinancingRequirementOverview from './FinancingRequirementOverview';
import { createFinancingRequirementDraft, getCurrentFinancingRequirement, getPendingFinancingRequirement } from '../../services/api';

vi.mock('../../stores/useChatStore', () => ({
  useChatStore: () => ({ aiContext: { selectedCustomer: { id: 'test-customer' } } }),
}));
vi.mock('../../services/api', () => ({
  getCurrentFinancingRequirement: vi.fn(),
  getPendingFinancingRequirement: vi.fn(),
  createFinancingRequirementDraft: vi.fn(),
  confirmFinancingRequirement: vi.fn(),
}));

describe('FinancingRequirementOverview', () => {
  beforeEach(() => vi.clearAllMocks());

  it('keeps missing state empty across reloads without creating a draft', async () => {
    vi.mocked(getPendingFinancingRequirement).mockResolvedValue(null);
    vi.mocked(getCurrentFinancingRequirement).mockResolvedValue(null);
    const view = render(<FinancingRequirementOverview />);
    await screen.findByText('当前尚未确认融资需求');
    view.unmount();
    render(<FinancingRequirementOverview />);
    await waitFor(() => expect(getPendingFinancingRequirement).toHaveBeenCalledTimes(2));
    expect(createFinancingRequirementDraft).not.toHaveBeenCalled();
  });

  it('restores a real pending draft without incrementing its version', async () => {
    vi.mocked(getPendingFinancingRequirement).mockResolvedValue({
      requirement_id: 'draft-1', customer_id: 'test-customer', version: 1, status: 'needs_confirmation',
      borrower_entity: '测试客户', requested_amount: 5_000_000, amount_confirmed: true,
      financing_purpose: '采购', purpose_detail: '材料采购', term_value: 12, term_unit: 'month', term_confirmed: true,
    });
    render(<FinancingRequirementOverview />);
    await screen.findByText('融资需求 V1');
    fireEvent(window, new Event('financing-requirement-updated'));
    await waitFor(() => expect(getPendingFinancingRequirement).toHaveBeenCalledTimes(2));
    expect(getCurrentFinancingRequirement).not.toHaveBeenCalled();
    expect(createFinancingRequirementDraft).not.toHaveBeenCalled();
  });
});
