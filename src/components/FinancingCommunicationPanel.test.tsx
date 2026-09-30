import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FinancingCommunicationPanel } from './FinancingCommunicationPanel';
import * as api from '../services/api';

vi.mock('../services/api', () => ({
  listApplicationContacts: vi.fn(), listApplicationCommunications: vi.fn(),
  listFinancingFollowUps: vi.fn(), getFinancingApplicationTimeline: vi.fn(),
  createFinancingContact: vi.fn(), attachApplicationContact: vi.fn(),
  createApplicationCommunication: vi.fn(), createTaskFromCommunication: vi.fn(),
  createSupplementFromCommunication: vi.fn(), createReviewFeedbackFromCommunication: vi.fn(),
  completeFinancingFollowUp: vi.fn(),
}));

const application = {
  application_id: 'a1', customer_id: 'c1', customer_name: '上海意川建筑科技有限公司',
  institution_name: '中国银行', product_name: '惠担贷', status: 'under_review', stages: [], tasks: [], events: [],
} as unknown as api.FinancingApplicationData;

const contact: api.FinancingContactData = {
  contact_id: 'c-bank', contact_type: 'institution', customer_id: null, institution_name: '中国银行',
  branch_name: '上海支行', name: '张经理', title: '客户经理', department: null,
  mobile: '138****5678', phone: null, email: 'z***@example.com', wechat: '已配置',
  is_primary: true, related_person_id: null, status: 'active', notes: '',
};

describe('FinancingCommunicationPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.listApplicationContacts).mockResolvedValue([contact]);
    vi.mocked(api.listApplicationCommunications).mockResolvedValue([]);
    vi.mocked(api.listFinancingFollowUps).mockResolvedValue([]);
    vi.mocked(api.getFinancingApplicationTimeline).mockResolvedValue([]);
  });

  it('renders contacts, communication timeline and follow-up areas', async () => {
    render(<FinancingCommunicationPanel application={application} canWrite onApplicationChanged={vi.fn()} />);
    expect(await screen.findByText('联系人与沟通跟进')).toBeTruthy();
    expect(screen.getAllByText(/张经理/).length).toBeGreaterThan(0);
    expect(screen.getByText('沟通记录')).toBeTruthy();
    expect(screen.getByText('跟进事项')).toBeTruthy();
    expect(screen.getByText('全部动态')).toBeTruthy();
  });

  it('saves a manually entered communication without sending it', async () => {
    vi.mocked(api.createApplicationCommunication).mockResolvedValue({} as api.FinancingCommunicationData);
    render(<FinancingCommunicationPanel application={application} canWrite onApplicationChanged={vi.fn()} />);
    fireEvent.change(await screen.findByLabelText('联系人'), { target: { value: 'c-bank' } });
    fireEvent.change(screen.getByLabelText('沟通主题'), { target: { value: '查询审批进度' } });
    fireEvent.change(screen.getByLabelText('沟通内容'), { target: { value: '银行正在审核。' } });
    fireEvent.click(screen.getByRole('button', { name: '保存沟通记录' }));
    await waitFor(() => expect(api.createApplicationCommunication).toHaveBeenCalledWith('a1', expect.objectContaining({ subject: '查询审批进度', content: '银行正在审核。' })));
  });

  it('keeps viewer communication areas read only', async () => {
    render(<FinancingCommunicationPanel application={application} canWrite={false} onApplicationChanged={vi.fn()} />);
    expect(await screen.findByText('当前账号只能查看沟通记录。')).toBeTruthy();
    expect(screen.queryByRole('button', { name: '保存沟通记录' })).toBeNull();
  });
});
