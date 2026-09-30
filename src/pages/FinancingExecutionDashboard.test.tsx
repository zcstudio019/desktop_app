import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import FinancingExecutionDashboard from './FinancingExecutionDashboard';
import { getFinancingExecutionDashboard, type FinancingExecutionDashboardData } from '../services/api';

vi.mock('../services/api', () => ({ getFinancingExecutionDashboard: vi.fn() }));

const empty: FinancingExecutionDashboardData = {
  total_applications: 0,
  status_counts: {},
  amounts: { target_amount: '0', submitted_amount: '0', approved_amount: '0', disbursed_amount: '0', pending_approval_amount: '0', undisbursed_amount: '0' },
  applications: [],
};

describe('FinancingExecutionDashboard', () => {
  beforeEach(() => vi.mocked(getFinancingExecutionDashboard).mockResolvedValue(empty));

  it('shows the Shanghai Yichuan empty execution state without fake data', async () => {
    render(<FinancingExecutionDashboard />);
    expect(await screen.findByText(/当前暂无融资执行申请。/)).toBeTruthy();
    expect(screen.getByText('总申请数').parentElement?.textContent).toContain('0');
  });

  it('renders factual amount totals and Chinese statuses', async () => {
    vi.mocked(getFinancingExecutionDashboard).mockResolvedValue({ ...empty, total_applications: 1, status_counts: { under_review: 1 }, amounts: { ...empty.amounts, target_amount: '8000000', submitted_amount: '8000000' } });
    render(<FinancingExecutionDashboard />);
    expect((await screen.findAllByText('800万元')).length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText('审批中').some(node => node.parentElement?.textContent?.includes('1'))).toBe(true);
  });

  it('supports status and overdue filters', async () => {
    render(<FinancingExecutionDashboard />);
    fireEvent.change(await screen.findByLabelText('申请状态'), { target: { value: 'supplement_required' } });
    fireEvent.click(screen.getByLabelText('仅看已逾期'));
    await waitFor(() => expect(getFinancingExecutionDashboard).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'supplement_required', overdue_only: true })));
  });

  it('does not render raw application status values', async () => {
    render(<FinancingExecutionDashboard />);
    await screen.findByText(/当前暂无融资执行申请。/);
    expect(screen.queryByText('under_review')).toBeNull();
  });
});


