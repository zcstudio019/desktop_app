import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FinancingRequirementCard } from './FinancingRequirementCard';
import { confirmFinancingRequirement, createFinancingRequirementDraft, type FinancingRequirementData } from '../services/api';

vi.mock('../services/api', () => ({
  confirmFinancingRequirement: vi.fn(),
  createFinancingRequirementDraft: vi.fn(),
}));

const draft: FinancingRequirementData = {
  requirement_id: 'r1', customer_id: 'c1', version: 1, status: 'needs_confirmation',
  borrower_entity: '上海意川建筑科技有限公司', requested_amount: 5_000_000,
  amount_confirmed: true, financing_purpose: '采购', purpose_detail: '材料采购',
  term_value: 12, term_unit: 'month', term_confirmed: true,
};

describe('FinancingRequirementCard', () => {
  beforeEach(() => vi.clearAllMocks());

  it('shows the extracted draft and confirms only through the explicit button', async () => {
    vi.mocked(confirmFinancingRequirement).mockResolvedValue({ ...draft, status: 'confirmed' });
    render(<FinancingRequirementCard initial={draft} />);
    expect(screen.getByText('状态：待确认')).toBeTruthy();
    expect(confirmFinancingRequirement).not.toHaveBeenCalled();
    fireEvent.click(screen.getByText('确认需求'));
    await waitFor(() => expect(confirmFinancingRequirement).toHaveBeenCalledWith('c1', 'r1'));
    expect(await screen.findByText('状态：已确认')).toBeTruthy();
  });

  it('creates a new draft version when edited', async () => {
    vi.mocked(createFinancingRequirementDraft).mockResolvedValue({ ...draft, requirement_id: 'r2', version: 2, requested_amount: 8_000_000 });
    render(<FinancingRequirementCard initial={{ ...draft, status: 'confirmed' }} />);
    fireEvent.click(screen.getByText('修改需求'));
    fireEvent.change(screen.getByLabelText('金额（万元）'), { target: { value: '800' } });
    fireEvent.click(screen.getByText('保存待确认版本'));
    await waitFor(() => expect(createFinancingRequirementDraft).toHaveBeenCalledWith('c1', expect.objectContaining({ requested_amount: 8_000_000 })));
    expect(await screen.findByText('融资需求 V2')).toBeTruthy();
  });

  it('cannot confirm a draft without the four required facts', () => {
    render(<FinancingRequirementCard initial={{ ...draft, term_value: null }} />);
    expect((screen.getByText('确认需求') as HTMLButtonElement).disabled).toBe(true);
  });
});
